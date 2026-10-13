# ------------------------------------------------------- Kinesis Data Streams ----
# Partition key = account ID (set by the producer), so one account's payments stay in order.
# Created with the AWS CLI rather than Terraform's aws_kinesis_stream resource. That resource
# always calls IncreaseStreamRetentionPeriod when it creates a stream, even for the default 24
# hours, and the Learner Lab forbids that call. The stream keeps the default 24-hour retention.
resource "terraform_data" "stream" {
  input            = { name = local.stream, region = var.region }
  triggers_replace = var.kinesis_shards # a different shard count makes a new stream

  provisioner "local-exec" {
    command = join(" ", [
      "aws kinesis describe-stream-summary --stream-name ${local.stream} --region ${var.region} --no-cli-pager --query StreamDescriptionSummary.StreamStatus",
      "|| aws kinesis create-stream --stream-name ${local.stream} --shard-count ${var.kinesis_shards} --region ${var.region}",
      "&& aws kinesis wait stream-exists --stream-name ${local.stream} --region ${var.region}",
    ])
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws kinesis delete-stream --stream-name ${self.input.name} --region ${self.input.region} && aws kinesis wait stream-not-exists --stream-name ${self.input.name} --region ${self.input.region}"
  }
}

locals {
  stream_name = terraform_data.stream.output.name
  stream_arn  = "arn:aws:kinesis:${var.region}:${local.account}:stream/${terraform_data.stream.output.name}"
}

# Spec 3.4: alarm when a consumer falls more than a minute behind the stream.
resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "aml-kinesis-iterator-age"
  alarm_description   = "A consumer of ${local.stream} is more than 60 seconds behind."
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  dimensions          = { StreamName = local.stream_name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 60000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

# ------------------------------------------------- Firehose: raw landing (spec 3.2) ----
# Every transaction as received, converted to Parquet and filed by its own date. No cleaning.
resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = "aml-raw-landing"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = local.stream_arn
    role_arn           = local.role_arn
  }

  extended_s3_configuration {
    role_arn            = local.role_arn
    bucket_arn          = "arn:aws:s3:::${terraform_data.bucket.output.bucket}"
    prefix              = "raw/transactions/date=!{partitionKeyFromQuery:txn_date}/"
    error_output_prefix = "raw-errors/!{firehose:error-output-type}/"
    buffering_size      = 64 # the minimum when dynamic partitioning is on
    buffering_interval  = 60

    dynamic_partitioning_configuration {
      enabled = true
    }

    processing_configuration {
      enabled = true
      processors {
        type = "MetadataExtraction"
        parameters {
          parameter_name  = "JsonParsingEngine"
          parameter_value = "JQ-1.6"
        }
        parameters {
          parameter_name  = "MetadataExtractionQuery"
          parameter_value = "{txn_date:(.txn_date // \"unknown\")}"
        }
      }
    }

    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }
      schema_configuration {
        database_name = aws_glue_catalog_database.aml.name
        table_name    = aws_glue_catalog_table.t["raw_transactions"].name
        role_arn      = local.role_arn
        region        = var.region
      }
    }
  }
}

# ---------------------------------------------------------- DynamoDB alerts (spec 3.2) ----
resource "aws_dynamodb_table" "alerts" {
  name         = local.alerts_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "txn_id"

  attribute {
    name = "txn_id"
    type = "S"
  }
  attribute {
    name = "status"
    type = "S"
  }
  attribute {
    name = "score"
    type = "N"
  }

  global_secondary_index {
    name            = "status-score-index"
    hash_key        = "status"
    range_key       = "score"
    projection_type = "ALL"
  }

  # Backstop only: the daily label export removes items after recording them in S3.
  ttl {
    attribute_name = "ttl_at"
    enabled        = true
  }
}

# ------------------------------------------------ Managed Apache Flink (spec 3.2) ----
resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/managed-flink/${local.flink_app}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_kinesisanalyticsv2_application" "q1" {
  name                   = local.flink_app
  runtime_environment    = var.flink_runtime
  service_execution_role = local.role_arn
  start_application      = var.start_flink

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = "arn:aws:s3:::${terraform_data.bucket.output.bucket}"
          file_key   = aws_s3_object.flink_zip.key
        }
      }
    }

    environment_properties {
      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"
        property_map = {
          python  = "main.py"
          jarfile = "lib/${local.schema.flink_jar}"
        }
      }
      property_group {
        property_group_id = "app"
        property_map = {
          "bucket"        = local.bucket
          "stream.name"   = local.stream_name
          "aws.region"    = var.region
          "our.bank"      = local.our_bank
          "alerts.table"  = aws_dynamodb_table.alerts.name
          "audit.rate"    = tostring(var.audit_rate)
          "init.position" = "LATEST"
          "seed.path"     = var.flink_seed ? "${local.s3}/state/flink_seed/" : ""
          "flush.seconds" = "30"
          "flush.rows"    = "5000"
          "sink.buckets"  = tostring(max(1, var.flink_parallelism))
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "DEFAULT" # checkpoint every minute
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.flink_parallelism
        parallelism_per_kpu  = 1
        auto_scaling_enabled = false
      }
    }

    application_snapshot_configuration {
      snapshots_enabled = true # a stop/start keeps the 7-day state
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }
}
