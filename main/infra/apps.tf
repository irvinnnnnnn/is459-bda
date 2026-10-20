# Both Streamlit apps on one small EC2 instance, reachable from your computer's browser.
#
#   Q1 investigator app   http://<public ip>:8501
#   Q2 KYC app            http://<public ip>:8502
#
# The instance copies the apps and config.json from S3 and runs each as a service, so they come
# back by themselves when the Learner Lab restarts the instance at the start of a session.
# The apps are open to anyone who has the address. They have no login and use plain http, so treat
# the address as the password: anyone with it can read the queues and record decisions. To limit
# access, set apps_allowed_cidr to your own address or a campus range.

variable "apps_on_ec2" {
  description = "Host both Streamlit apps on an EC2 instance. Set false to run them only on your laptop."
  type        = bool
  default     = true
}

variable "apps_instance_type" {
  description = "t3.small (2 GB, about $0.50 a day) runs both apps comfortably. t3.micro is half the price but tight on memory."
  type        = string
  default     = "t3.small"
}

variable "apps_allowed_cidr" {
  description = "Who may open the apps, as a CIDR. Default: anyone. Narrow it to e.g. \"203.0.113.7/32\" to allow one address only."
  type        = string
  default     = "0.0.0.0/0"
}

variable "apps_instance_profile" {
  description = "EC2 instance profile used by the Streamlit apps."
  type        = string
  default     = "LabInstanceProfile"
}

variable "create_apps_instance_profile" {
  description = "Create the app EC2 instance profile around role_name. Set false if this profile already exists."
  type        = bool
  default     = true
}

data "aws_vpc" "default" {
  count   = var.apps_on_ec2 ? 1 : 0
  default = true
}

data "aws_subnet" "apps" {
  count             = var.apps_on_ec2 ? 1 : 0
  availability_zone = "${var.region}a"
  default_for_az    = true
}

data "aws_ssm_parameter" "al2023" {
  count = var.apps_on_ec2 ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

locals {
  # Laid out on the instance exactly as in the repo, so each app finds config.json two folders up.
  app_files = {
    "Q1_files/streamlit/q1_streamlit.py"        = "${local.main}/Q1_files/streamlit/q1_streamlit.py"
    "Q1_files/streamlit/.streamlit/config.toml" = "${local.main}/Q1_files/streamlit/.streamlit/config.toml"
    "Q2_files/streamlit/q2_streamlit.py"        = "${local.main}/Q2_files/streamlit/q2_streamlit.py"
    "Q2_files/streamlit/.streamlit/config.toml" = "${local.main}/Q2_files/streamlit/.streamlit/config.toml"
  }
  # Any change to an app or to the resource names gives a new hash, which replaces the instance.
  apps_version = md5(join(",", concat([for k, v in local.app_files : filemd5(v)], [jsonencode(local.config)])))

  apps = {
    q1 = { dir = "Q1_files/streamlit", file = "q1_streamlit.py", port = 8501, mode = "Q1_MODE" }
    q2 = { dir = "Q2_files/streamlit", file = "q2_streamlit.py", port = 8502, mode = "Q2_MODE" }
  }
}

resource "aws_s3_object" "app" {
  for_each    = var.apps_on_ec2 ? local.app_files : {}
  bucket      = terraform_data.bucket.output.bucket
  key         = "code/app/${each.key}"
  source      = each.value
  source_hash = filemd5(each.value)
}

resource "aws_s3_object" "app_config" {
  count   = var.apps_on_ec2 ? 1 : 0
  bucket  = terraform_data.bucket.output.bucket
  key     = "code/app/config.json"
  content = jsonencode(local.config)
}

resource "aws_security_group" "apps" {
  count       = var.apps_on_ec2 ? 1 : 0
  name        = "aml-streamlit-apps"
  description = "Streamlit apps: ports 8501 and 8502"
  vpc_id      = data.aws_vpc.default[0].id

  ingress {
    description = "Q1 and Q2 apps"
    from_port   = 8501
    to_port     = 8502
    protocol    = "tcp"
    cidr_blocks = [var.apps_allowed_cidr]
  }
  egress {
    description = "Out to AWS APIs and package downloads"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "apps" {
  count                       = var.apps_on_ec2 ? 1 : 0
  ami                         = data.aws_ssm_parameter.al2023[0].value
  instance_type               = var.apps_instance_type
  subnet_id                   = data.aws_subnet.apps[0].id
  vpc_security_group_ids      = [aws_security_group.apps[0].id]
  iam_instance_profile        = var.apps_instance_profile
  associate_public_ip_address = true
  user_data_replace_on_change = true
  tags                        = { Name = "aml-streamlit-apps" }

  metadata_options {
    http_tokens = "required"
  }

  user_data = <<-EOT
    #!/bin/bash
    # apps version ${local.apps_version}
    set -eux
    dnf install -y python3.11 python3.11-pip
    python3.11 -m venv /opt/aml/venv
    /opt/aml/venv/bin/pip install --no-cache-dir streamlit boto3 pandas altair pyarrow
    mkdir -p /opt/aml/main
    chown -R ec2-user:ec2-user /opt/aml
    %{for name, app in local.apps~}
    cat > /etc/systemd/system/aml-${name}.service <<'UNIT'
    [Unit]
    Description=AML ${name} Streamlit app
    After=network-online.target
    Wants=network-online.target

    [Service]
    User=ec2-user
    Environment=${app.mode}=aws
    Environment=AWS_DEFAULT_REGION=${var.region}
    WorkingDirectory=/opt/aml/main/${app.dir}
    ExecStartPre=/usr/bin/aws s3 sync s3://${local.bucket}/code/app/ /opt/aml/main/ --region ${var.region}
    ExecStart=/opt/aml/venv/bin/streamlit run ${app.file} --server.address 0.0.0.0 --server.port ${app.port} --server.headless true --browser.gatherUsageStats false
    Restart=always
    RestartSec=5

    [Install]
    WantedBy=multi-user.target
    UNIT
    %{endfor~}
    sudo -u ec2-user /usr/bin/aws s3 sync s3://${local.bucket}/code/app/ /opt/aml/main/ --region ${var.region}
    systemctl daemon-reload
    systemctl enable --now aml-q1 aml-q2
  EOT

  depends_on = [aws_s3_object.app, aws_s3_object.app_config]
}

output "apps" {
  description = "Where the apps are. The address changes when the instance restarts: run `python main/scripts/pipeline.py apps` for the current one."
  value = var.apps_on_ec2 ? {
    q1_investigators = "http://${aws_instance.apps[0].public_ip}:8501"
    q2_kyc           = "http://${aws_instance.apps[0].public_ip}:8502"
    open_to          = var.apps_allowed_cidr
  } : null
}
