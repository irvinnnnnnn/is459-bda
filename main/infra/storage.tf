# ---------------------------------------------------------------- S3 (spec 2.2) ----
# The bucket is created with the AWS CLI rather than Terraform's aws_s3_bucket resource. That
# resource always reads the bucket's object-lock setting, and the Learner Lab forbids that call
# (s3:GetBucketObjectLockConfiguration), so it fails there even though the bucket itself is fine.
# New buckets block public access by default, so no extra setting is needed.
resource "terraform_data" "bucket" {
  input = { bucket = local.bucket, region = var.region }

  # head-bucket succeeds if we already own it, so applying twice is harmless.
  provisioner "local-exec" {
    command = join(" ", [
      "aws s3api head-bucket --bucket ${local.bucket} --region ${var.region}",
      "|| aws s3api create-bucket --bucket ${local.bucket} --region ${var.region}",
      var.region == "us-east-1" ? "" : "--create-bucket-configuration LocationConstraint=${var.region}",
    ])
  }

  # terraform destroy: empty the bucket, then remove it.
  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rb s3://${self.input.bucket} --force --region ${self.input.region}"
  }
}

# Code the jobs run. A changed file is re-uploaded on the next apply.
locals {
  code = {
    "code/common/aml_clean.py"            = "${local.main}/common/aml_clean.py"
    "code/q1/q1_core.py"                  = "${local.main}/Q1_files/q1_core.py"
    "code/q2/q2_common.py"                = "${local.main}/Q2_files/q2_common.py"
    "code/common/spark_common.py"         = "${local.main}/common/spark_common.py"
    "code/glue/q1_features_historical.py" = "${local.main}/Q1_files/glue/q1_features_historical.py"
    "code/glue/q1_label_export.py"        = "${local.main}/Q1_files/glue/q1_label_export.py"
    "code/glue/q1_build_training.py"      = "${local.main}/Q1_files/glue/q1_build_training.py"
    "code/glue/q2_etl.py"                 = "${local.main}/Q2_files/glue/q2_etl.py"
    "code/emr/q2_score.py"                = "${local.main}/Q2_files/emr/q2_score.py"
    "code/emr/bootstrap.sh"               = "${local.main}/Q2_files/emr/bootstrap.sh"
    "code/sagemaker/evaluate/evaluate.py" = "${local.main}/Q1_files/sagemaker/evaluate.py"
    "code/sagemaker/train.tar.gz"         = "${local.build}/train.tar.gz"
  }
  # The Flink zip's key carries its hash: a new build is a new object, which makes Flink redeploy.
  flink_zip = "code/flink/q1-flink-${filemd5("${local.build}/q1-flink.zip")}.zip"
}

resource "aws_s3_object" "code" {
  for_each    = local.code
  bucket      = terraform_data.bucket.output.bucket
  key         = each.key
  source      = each.value
  source_hash = filemd5(each.value)
}

resource "aws_s3_object" "flink_zip" {
  bucket      = terraform_data.bucket.output.bucket
  key         = local.flink_zip
  source      = "${local.build}/q1-flink.zip"
  source_hash = filemd5("${local.build}/q1-flink.zip")
}

# ------------------------------------------- Glue Data Catalog (spec 2.1, no crawlers) ----
# Every schema is written here. Partitions are found by Athena partition projection, so nothing
# has to register them: a new date folder is queryable as soon as it has a file.
resource "aws_glue_catalog_database" "aml" {
  name = local.database
}

locals {
  txn_columns = [
    ["txn_id", "string"], ["txn_ts", "timestamp"], ["from_bank", "string"], ["from_account", "string"],
    ["to_bank", "string"], ["to_account", "string"], ["amount_paid", "double"], ["payment_currency", "string"],
    ["amount_received", "double"], ["receiving_currency", "string"], ["payment_format", "string"],
  ]
  # Glue stores column names in lower case (s_out_n_1D -> s_out_n_1d). Sending them lower-cased
  # already stops every plan from showing these tables as changed. Athena matches names ignoring case.
  feature_columns = [for f in local.schema.features : [lower(f), "double"]]
  scored_columns  = [for c in local.schema.scored_columns : [lower(c[0]), c[1]]]

  tables = {
    # Firehose converts to Parquet against this table, so it must exist before Firehose starts.
    # Every column is a string: the landing zone stores what arrived and cleans nothing.
    raw_transactions = {
      location  = "raw/transactions/"
      partition = "date"
      columns = [for c in ["txn_id", "txn_ts", "from_bank", "from_account", "to_bank", "to_account", "amount_paid",
      "payment_currency", "amount_received", "receiving_currency", "payment_format", "is_laundering"] : [c, "string"]]
    }
    live_scored = {
      location  = "curated/live/scored/"
      partition = "date"
      columns   = local.scored_columns
    }
    live_labels = {
      location  = "curated/live/labels/"
      partition = "export_date"
      columns = [["txn_id", "string"], ["outcome", "string"], ["is_audit", "boolean"], ["reviewer", "string"],
        ["reason", "string"], ["note", "string"], ["decided_at", "timestamp"], ["created_at", "timestamp"],
        ["expires_at", "timestamp"], ["score", "double"], ["threshold", "double"], ["model_version", "string"],
      ["exported_at", "timestamp"]]
    }
    curated_historical = {
      location  = "curated/historical/"
      partition = "date"
      columns   = concat(local.txn_columns, [["label", "int"]], local.feature_columns)
    }
    customer_txns = {
      location  = "curated/customer_txns/"
      partition = "date"
      columns = concat(local.txn_columns, [["is_laundering", "int"], ["source", "string"],
        ["from_customer_id", "string"], ["from_entity_type", "string"], ["to_customer_id", "string"],
      ["to_entity_type", "string"], ["amount_paid_usd", "double"], ["amount_received_usd", "double"]])
    }
    decisions = {
      location  = "decisions/"
      partition = "date"
      columns = [["customer_id", "string"], ["event", "string"], ["analyst", "string"], ["at", "timestamp"],
        ["cleared_until", "string"], ["note", "string"], ["snapshot_score", "double"], ["snapshot_rules", "string"],
      ["run_date", "string"], ["written_at", "timestamp"]]
    }
    scores = {
      location  = "scores/"
      partition = "run_date"
      columns = [["customer_id", "string"], ["entity_type", "string"], ["n_accounts", "int"], ["accounts", "string"],
        ["points", "int"], ["peer", "double"], ["score", "double"], ["tier", "string"], ["rules", "string"],
        ["hits", "string"], ["in_queue", "boolean"], ["queue_reason", "string"], ["prev_score", "double"],
        ["change", "double"], ["new_rules", "string"], ["metrics", "string"], ["laundering", "int"],
      ["window_start", "string"], ["window_complete", "boolean"]]
    }
    rule_hits = {
      location  = "rule_hits/"
      partition = "run_date"
      columns   = [["customer_id", "string"], ["rule", "string"], ["points", "int"], ["evidence", "string"]]
    }
    rule_eval = {
      location  = "rule_eval/"
      partition = "run_date"
      columns = [["kind", "string"], ["key", "string"], ["customers", "bigint"], ["with_laundering", "bigint"],
      ["precision", "double"]]
    }
  }
}

resource "aws_glue_catalog_table" "t" {
  for_each      = local.tables
  database_name = aws_glue_catalog_database.aml.name
  name          = each.key
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"                                   = "parquet"
    "projection.enabled"                               = "true"
    "projection.${each.value.partition}.type"          = "date"
    "projection.${each.value.partition}.format"        = "yyyy-MM-dd"
    "projection.${each.value.partition}.range"         = "2000-01-01,NOW+1DAYS"
    "projection.${each.value.partition}.interval"      = "1"
    "projection.${each.value.partition}.interval.unit" = "DAYS"
  }

  partition_keys {
    name = each.value.partition
    type = "string"
  }

  storage_descriptor {
    location      = "${local.s3}/${each.value.location}"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}

# -------------------------------------------------------------------- Athena ----
resource "aws_athena_workgroup" "aml" {
  name          = "aml"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "${local.s3}/athena-results/"
    }
  }
}
