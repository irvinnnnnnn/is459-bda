# One shared execution role is used by the pipeline services and (optionally) the app EC2
# instance, matching the repository's role_name setting.
resource "aws_iam_role" "pipeline" {
  count = var.manage_iam_resources ? 1 : 0
  name  = var.role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = [
        "ec2.amazonaws.com",
        "events.amazonaws.com",
        "firehose.amazonaws.com",
        "glue.amazonaws.com",
        "kinesisanalytics.amazonaws.com",
        "lambda.amazonaws.com",
        "sagemaker.amazonaws.com",
        "states.amazonaws.com"
      ] }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "pipeline" {
  count = var.manage_iam_resources ? 1 : 0
  name  = "is459-aml-runtime"
  role  = aws_iam_role.pipeline[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          # DeleteObject: Spark's overwrite writes and its _temporary/ cleanup delete files
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation",
          "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
        ]
        Resource = ["arn:aws:s3:::${local.bucket}", "arn:aws:s3:::${local.bucket}/*"]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem", "dynamodb:DeleteItem",
          "dynamodb:DescribeTable", "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query",
          "dynamodb:Scan", "dynamodb:UpdateItem"
        ]
        Resource = "arn:aws:dynamodb:${var.region}:${local.account}:table/${local.alerts_table}"
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords",
          "kinesis:GetShardIterator", "kinesis:ListShards", "kinesis:PutRecord", "kinesis:PutRecords",
          "kinesis:SubscribeToShard"
        ]
        Resource = "arn:aws:kinesis:${var.region}:${local.account}:stream/${local.stream}"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
          "glue:GetTableVersion", "glue:GetTableVersions", "glue:GetPartition", "glue:GetPartitions",
          "glue:BatchGetPartition", "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
          "athena:StopQueryExecution", "athena:GetWorkGroup"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:DescribeLogGroups", "logs:DescribeLogStreams",
          "logs:PutLogEvents"
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${local.account}:log-group:/aws/*",
          "arn:aws:logs:${var.region}:${local.account}:log-group:/aws-glue/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:GetMetricStatistics", "cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        # Pulling the AWS scikit-learn container for SageMaker training and evaluation
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken", "ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = "*"
      },
      {
        # Training (createTrainingJob.sync), evaluation (createProcessingJob.sync) and the model registry
        Effect = "Allow"
        Action = [
          "sagemaker:CreateTrainingJob", "sagemaker:DescribeTrainingJob", "sagemaker:StopTrainingJob",
          "sagemaker:CreateProcessingJob", "sagemaker:DescribeProcessingJob", "sagemaker:StopProcessingJob",
          "sagemaker:CreateModelPackage", "sagemaker:DescribeModelPackage", "sagemaker:ListModelPackages",
          "sagemaker:AddTags", "sagemaker:ListTags", "sagemaker:CreateModel"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution", "states:DescribeExecution", "states:ListExecutions",
          "lambda:InvokeFunction", "events:PutRule", "events:PutTargets", "events:DescribeRule",
          "events:DeleteRule", "events:RemoveTargets"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "elasticmapreduce:RunJobFlow", "elasticmapreduce:DescribeCluster", "elasticmapreduce:AddJobFlowSteps",
          "elasticmapreduce:DescribeStep", "elasticmapreduce:CancelSteps", "elasticmapreduce:ListSteps",
          "elasticmapreduce:TerminateJobFlows", "elasticmapreduce:AddTags"
        ]
        Resource = "*"
      },
      {
        # A new account has no EMR cleanup service-linked role yet; the first cluster launch creates it
        Effect    = "Allow"
        Action    = "iam:CreateServiceLinkedRole"
        Resource  = "arn:aws:iam::${local.account}:role/aws-service-role/*"
        Condition = { StringLike = { "iam:AWSServiceName" = ["elasticmapreduce.amazonaws.com", "spot.amazonaws.com"] } }
      },
      {
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = [
          "arn:aws:iam::${local.account}:role/${var.role_name}",
          "arn:aws:iam::${local.account}:role/${var.emr_service_role}",
          "arn:aws:iam::${local.account}:role/${var.emr_instance_profile}"
        ]
        Condition = { StringLike = { "iam:PassedToService" = [
          "firehose.amazonaws.com", "glue.amazonaws.com", "kinesisanalytics.amazonaws.com",
          "lambda.amazonaws.com", "sagemaker.amazonaws.com", "states.amazonaws.com",
          "elasticmapreduce.amazonaws.com", "ec2.amazonaws.com"
        ] } }
      }
    ]
  })
}

resource "aws_iam_instance_profile" "apps" {
  count = var.manage_iam_resources && var.apps_on_ec2 && var.create_apps_instance_profile ? 1 : 0
  name  = var.apps_instance_profile
  role  = aws_iam_role.pipeline[0].name
}

# Q2 EMR uses a service role to create/manage the cluster, and a separate EC2 instance
# profile for the Spark machines. The latter is limited to this project's S3 bucket.
resource "aws_iam_role" "emr_service" {
  count = var.manage_iam_resources ? 1 : 0
  name  = var.emr_service_role

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  count      = var.manage_iam_resources ? 1 : 0
  role       = aws_iam_role.emr_service[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2" {
  count = var.manage_iam_resources ? 1 : 0
  name  = var.emr_instance_profile

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "emr_ec2" {
  count = var.manage_iam_resources ? 1 : 0
  name  = "is459-aml-emr-data"
  role  = aws_iam_role.emr_ec2[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = "arn:aws:s3:::${local.bucket}"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = "arn:aws:s3:::${local.bucket}/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "emr_ec2" {
  count = var.manage_iam_resources ? 1 : 0
  name  = var.emr_instance_profile
  role  = aws_iam_role.emr_ec2[0].name
}

moved {
  from = aws_iam_role.pipeline
  to   = aws_iam_role.pipeline[0]
}

moved {
  from = aws_iam_role_policy.pipeline
  to   = aws_iam_role_policy.pipeline[0]
}

moved {
  from = aws_iam_role.emr_service
  to   = aws_iam_role.emr_service[0]
}

moved {
  from = aws_iam_role_policy_attachment.emr_service
  to   = aws_iam_role_policy_attachment.emr_service[0]
}

moved {
  from = aws_iam_role.emr_ec2
  to   = aws_iam_role.emr_ec2[0]
}

moved {
  from = aws_iam_role_policy.emr_ec2
  to   = aws_iam_role_policy.emr_ec2[0]
}

moved {
  from = aws_iam_instance_profile.emr_ec2
  to   = aws_iam_instance_profile.emr_ec2[0]
}
