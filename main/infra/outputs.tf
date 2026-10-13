locals {
  config = {
    region               = var.region
    bucket               = local.bucket
    our_bank             = local.our_bank
    live_start           = local.live_start
    stream               = local.stream_name
    alerts_table         = aws_dynamodb_table.alerts.name
    athena_db            = aws_glue_catalog_database.aml.name
    athena_workgroup     = aws_athena_workgroup.aml.name
    flink_app            = aws_kinesisanalyticsv2_application.q1.name
    features_job         = aws_glue_job.q1_features.name
    label_export_job     = aws_glue_job.q1_label_export.name
    q1_state_machine_arn = aws_sfn_state_machine.q1_train.arn
    q2_state_machine_arn = aws_sfn_state_machine.q2_daily.arn
  }
}

# The scripts and both Streamlit apps read this file, so resource names are never typed by hand.
resource "local_file" "config" {
  filename = "${local.main}/config.json"
  content  = jsonencode(merge(local.config, { apps_instance_id = try(aws_instance.apps[0].id, "") }))
}

output "config" {
  value = local.config
}

output "next_steps" {
  value = <<-EOT
    1. python main/producer/producer.py prepare --csv <HI-Small_Trans.csv>
    2. python main/producer/producer.py upload --accounts <HI-Small_accounts.csv> --fx <fx_rates.csv>
    3. python main/scripts/pipeline.py setup        (history features, first model)
    4. python main/scripts/pipeline.py start        (Flink)
    5. python main/producer/producer.py replay --speed 60 --labels --q2 async
    When you finish: python main/scripts/pipeline.py stop
  EOT
}
