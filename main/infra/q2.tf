# --------------------------------------------------------- Q2 Glue job (spec 4.3) ----
resource "aws_glue_job" "q2_etl" {
  name              = "q2-etl"
  role_arn          = local.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  command {
    script_location = "${local.s3}/${aws_s3_object.code["code/glue/q2_etl.py"].key}"
    python_version  = "3"
  }
  default_arguments = merge(local.glue_args, {
    "--extra-py-files" = local.py_files_q2
    "--our_bank"       = local.our_bank
    "--live_start"     = local.live_start
    "--run_date"       = local.live_start
  })
}

# ------------------------------------------------------ which day is the run for ----
data "archive_file" "resolve_run_date" {
  type        = "zip"
  source_file = "${path.module}/lambda/resolve_run_date.py"
  output_path = "${local.build}/resolve_run_date.zip"
}

resource "aws_lambda_function" "resolve_run_date" {
  function_name    = "q2-resolve-run-date"
  role             = local.role_arn
  runtime          = "python3.12"
  handler          = "resolve_run_date.handler"
  filename         = data.archive_file.resolve_run_date.output_path
  source_code_hash = data.archive_file.resolve_run_date.output_base64sha256
  timeout          = 15
  environment {
    variables = { BUCKET = local.bucket }
  }
}

# ---------------------------- Step Functions: Glue, then transient EMR (spec 4.6) ----
resource "aws_sfn_state_machine" "q2_daily" {
  name     = "q2-daily-kyc-scoring"
  role_arn = local.role_arn
  definition = templatefile("${path.module}/sfn/q2_daily.asl.json", {
    bucket               = local.bucket
    our_bank             = local.our_bank
    py_files             = local.py_files_q2
    etl_job              = aws_glue_job.q2_etl.name
    resolve_fn           = aws_lambda_function.resolve_run_date.function_name
    emr_release          = var.emr_release
    emr_service_role     = var.emr_service_role
    emr_instance_profile = var.emr_instance_profile
    instance_type        = var.emr_instance_type
    core_count           = var.emr_core_count
    core_market          = var.emr_use_spot ? "SPOT" : "ON_DEMAND"
    subnet               = local.emr_subnet
    struct_threshold     = tostring(var.struct_threshold_usd)
  })
  depends_on = [aws_s3_object.code]
}
