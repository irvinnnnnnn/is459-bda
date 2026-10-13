# Real-time schedules (spec 3.2 and 4.6). They are created disabled: in the demo the producer's
# simulated clock starts the daily jobs at each simulated midnight (producer.py --labels --q2).
# Set enable_schedules = true for a deployment that runs on the real clock.
locals {
  schedule_state = var.enable_schedules ? "ENABLED" : "DISABLED"
}

# Q2: every day, after the day's payments have landed.
resource "aws_cloudwatch_event_rule" "q2_daily" {
  name                = "q2-daily"
  description         = "Daily KYC scoring"
  schedule_expression = "cron(30 16 * * ? *)" # 00:30 Singapore time
  state               = local.schedule_state
}

resource "aws_cloudwatch_event_target" "q2_daily" {
  rule     = aws_cloudwatch_event_rule.q2_daily.name
  arn      = aws_sfn_state_machine.q2_daily.arn
  role_arn = local.role_arn
  input    = "{}"
}

# Q1: retrain every 6 months.
resource "aws_cloudwatch_event_rule" "q1_retrain" {
  name                = "q1-retrain"
  description         = "Retrain and, if better, promote the Q1 model"
  schedule_expression = "cron(0 18 1 1,7 ? *)"
  state               = local.schedule_state
}

resource "aws_cloudwatch_event_target" "q1_retrain" {
  rule     = aws_cloudwatch_event_rule.q1_retrain.name
  arn      = aws_sfn_state_machine.q1_train.arn
  role_arn = local.role_arn
  input    = "{}"
}

# Q1: daily label export. A Glue job is started by a Glue trigger, Glue's own scheduler;
# an EventBridge rule cannot target a Glue job directly.
resource "aws_glue_trigger" "q1_label_export" {
  name     = "q1-label-export-daily"
  type     = "SCHEDULED"
  schedule = "cron(0 16 * * ? *)"
  enabled  = var.enable_schedules
  actions {
    job_name = aws_glue_job.q1_label_export.name
  }
}
