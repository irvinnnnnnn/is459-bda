# ------------------------------------------------------------ Q1 Glue jobs (spec 3.2) ----
locals {
  glue_args = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "${local.s3}/tmp/glue/"
    "--bucket"                           = local.bucket
  }
}

# Historical lane: historical/ -> curated/historical/, the Flink seed, and the parity check.
resource "aws_glue_job" "q1_features" {
  name              = "q1-features-historical"
  role_arn          = local.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  command {
    script_location = "${local.s3}/${aws_s3_object.code["code/glue/q1_features_historical.py"].key}"
    python_version  = "3"
  }
  default_arguments = merge(local.glue_args, {
    "--extra-py-files" = local.py_files_q1
    "--our_bank"       = local.our_bank
    "--live_start"     = local.live_start
    "--seed_days"      = "7"
  })
}

# Training workflow step 1.
resource "aws_glue_job" "q1_build_training" {
  name              = "q1-build-training"
  role_arn          = local.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  command {
    script_location = "${local.s3}/${aws_s3_object.code["code/glue/q1_build_training.py"].key}"
    python_version  = "3"
  }
  default_arguments = merge(local.glue_args, {
    "--extra-py-files" = local.py_files_q1
    "--months"         = tostring(var.training_months)
    "--run_id"         = "manual"
  })
}

# Daily label export: DynamoDB -> curated/live/labels/. A small Python shell job, no Spark needed.
resource "aws_glue_job" "q1_label_export" {
  name         = "q1-label-export"
  role_arn     = local.role_arn
  max_capacity = 0.0625
  timeout      = 30
  command {
    name            = "pythonshell"
    script_location = "${local.s3}/${aws_s3_object.code["code/glue/q1_label_export.py"].key}"
    python_version  = "3.9"
  }
  default_arguments = {
    "library-set"    = "analytics" # pyarrow
    "--bucket"       = local.bucket
    "--alerts_table" = aws_dynamodb_table.alerts.name
    "--region"       = var.region
  }
}

# ------------------------------------------------ SageMaker Model Registry (spec 3.2) ----
resource "aws_sagemaker_model_package_group" "q1" {
  model_package_group_name        = "q1-aml-flagging"
  model_package_group_description = "Approved Q1 laundering-flagging models: artifact, training data and metrics per version."
}

# terraform destroy: AWS refuses to delete a group that still has versions, so delete them first.
resource "terraform_data" "q1_model_versions" {
  input      = { group = aws_sagemaker_model_package_group.q1.model_package_group_name, region = var.region }
  depends_on = [aws_sagemaker_model_package_group.q1]

  provisioner "local-exec" {
    when    = destroy
    # A script file, not `python -c "..."`: cmd.exe on Windows mangles the nested quotes.
    command = "python ${path.module}/../scripts/delete_model_versions.py ${self.input.group} ${self.input.region}"
  }
}

# ------------------------------------- Step Functions: train and promote (spec 3.2) ----
resource "aws_sfn_state_machine" "q1_train" {
  name     = "q1-train-and-promote"
  role_arn = local.role_arn
  definition = templatefile("${path.module}/sfn/q1_train.asl.json", {
    bucket      = local.bucket
    role_arn    = local.role_arn
    our_bank    = local.our_bank
    image       = var.sagemaker_image
    instance    = var.sagemaker_instance
    build_job   = aws_glue_job.q1_build_training.name
    model_group = aws_sagemaker_model_package_group.q1.model_package_group_name
  })
  depends_on = [aws_s3_object.code]
}
