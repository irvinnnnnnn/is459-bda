variable "region" {
  description = "AWS region. The Learner Lab only allows us-east-1 and us-west-2."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "S3 bucket for the whole project. Bucket names are global, so the default adds your account ID."
  type        = string
  default     = null
}

variable "role_name" {
  description = "Shared IAM execution role for the pipeline services and optional app EC2 instance."
  type        = string
  default     = "LabRole"
}

# ---- Q1 streaming
variable "kinesis_shards" {
  description = "Shards in the transaction stream. One shard takes 1,000 records or 1 MB a second."
  type        = number
  default     = 1
}

variable "flink_parallelism" {
  description = "Flink parallelism. Each unit is one KPU (about $0.11 an hour), plus one KPU for the job manager."
  type        = number
  default     = 1
}

variable "flink_runtime" {
  type    = string
  default = "FLINK-1_19"
}

variable "start_flink" {
  description = "Start the Flink application on apply. Leave false and use scripts/pipeline.py so it only runs when you demo."
  type        = bool
  default     = false
}

variable "flink_seed" {
  description = "At first start, load the last 7 days of history from state/flink_seed/ into Flink's state."
  type        = bool
  default     = true
}

variable "audit_rate" {
  description = "Share of unflagged payments drawn into the audit sample."
  type        = number
  default     = 0.003
}

variable "training_months" {
  description = "N: the rolling window of months kept in each training set."
  type        = number
  default     = 6
}

variable "sagemaker_instance" {
  description = "The Learner Lab allows medium, large and xlarge SageMaker instances only."
  type        = string
  default     = "ml.m5.large"
}

variable "sagemaker_image" {
  description = "scikit-learn container for training and evaluation. The account ID in the URI differs by region; this one is us-east-1."
  type        = string
  default     = "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3"
}

# ---- Q2 nightly batch
variable "emr_release" {
  type    = string
  default = "emr-7.1.0"
}

variable "emr_instance_type" {
  type    = string
  default = "m5.xlarge"
}

variable "emr_core_count" {
  description = "Core nodes. With the master that is emr_core_count + 1 instances; the Learner Lab allows 9 in total."
  type        = number
  default     = 2
}

variable "emr_use_spot" {
  description = "Spot core nodes, as in the spec. The Learner Lab is on-demand only, so this is off by default."
  type        = bool
  default     = false
}

variable "emr_service_role" {
  type    = string
  default = "EMR_DefaultRole"
}

variable "emr_instance_profile" {
  type    = string
  default = "EMR_EC2_DefaultRole"
}

variable "emr_subnet_id" {
  description = "Subnet for the EMR cluster. Default: the default VPC's subnet in the region's first availability zone."
  type        = string
  default     = null
}

variable "struct_threshold_usd" {
  description = "R6 structuring threshold in USD: MAS Notice 626's S$20,000 at the September 2022 rate."
  type        = number
  default     = 14300
}

variable "enable_schedules" {
  description = "Turn on the real-time schedules (daily Q2 run, daily label export, 6-monthly retrain). Off for the demo, where the producer's simulated clock starts them."
  type        = bool
  default     = false
}
