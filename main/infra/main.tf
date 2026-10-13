# IS459 AML project: everything in PIPELINE_SPEC.md for Q1 and Q2, in one apply.
#
#   python main/scripts/build.py        package the code (main/build/)
#   cd main/infra
#   terraform init
#   terraform apply
#
# Files: storage.tf (S3, code upload, Glue Data Catalog, Athena)   q1_streaming.tf (Kinesis, Firehose,
# Flink, DynamoDB)   q1_training.tf (Glue jobs, SageMaker, Step Functions)   q2.tf (Glue, EMR,
# Step Functions)   schedules.tf   outputs.tf (also writes main/config.json for the scripts and apps)

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.60" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
    local   = { source = "hashicorp/local", version = "~> 2.5" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "is459-aml" }
  }
}

data "aws_caller_identity" "me" {}

data "aws_subnet" "emr" {
  count             = var.emr_subnet_id == null ? 1 : 0
  availability_zone = "${var.region}a"
  default_for_az    = true
}

locals {
  account  = data.aws_caller_identity.me.account_id
  bucket   = coalesce(var.bucket_name, "is459-aml-${local.account}")
  role_arn = "arn:aws:iam::${local.account}:role/${var.role_name}"
  s3       = "s3://${local.bucket}"
  main     = "${path.module}/.."
  build    = "${path.module}/../build"
  schema   = jsondecode(file("${local.build}/schema.json"))

  # Whose view the pipelines take, and where "live" starts: set once, in main/common/settings.json.
  settings   = jsondecode(file("${path.module}/../common/settings.json"))
  our_bank   = local.settings.our_bank
  live_start = local.settings.live_start

  # Not "aml-transactions" / "q1-alerts": the first version's hand-made resources may still use those names.
  stream       = "aml-payments"
  alerts_table = "aml-q1-alerts"
  database     = "aml"
  flink_app    = "q1-realtime-flagging"
  emr_subnet   = coalesce(var.emr_subnet_id, try(data.aws_subnet.emr[0].id, null))

  # Modules shipped with the Spark jobs. common/ is what both pipelines share (the cleaning rules
  # Flink also runs); each pipeline adds only its own module.
  py_common   = "${local.s3}/code/common/aml_clean.py,${local.s3}/code/common/spark_common.py"
  py_files_q1 = "${local.py_common},${local.s3}/code/q1/q1_core.py"
  py_files_q2 = "${local.py_common},${local.s3}/code/q2/q2_common.py"
}
