variable "project"           { default = "lakehouse" }
variable "firehose_name"     { default = "firehose-orders" }
variable "buffer_size_mb"    { default = 1 }   # 1–128
variable "buffer_interval_s" { default = 60 }  # 60–900

############################################
# CloudWatch Logs for Firehose diagnostics
############################################
resource "aws_cloudwatch_log_group" "fh_lg" {
  name              = "/aws/kinesisfirehose/${var.firehose_name}"
  retention_in_days = 14
  tags = {
    Project = var.project
  }
}

resource "aws_cloudwatch_log_stream" "fh_ls" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.fh_lg.name
}

############################################
# IAM role that Firehose assumes
############################################
data "aws_iam_policy_document" "assume_firehose" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.project}-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.assume_firehose.json
  tags = {
    Project = var.project
  }
}

# Least-privilege policy so Firehose can write to S3, log to CWL, and use KMS
data "aws_iam_policy_document" "firehose_policy" {
  statement {
    sid     = "S3Write"
    effect  = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.dst.arn,
      "${aws_s3_bucket.dst.arn}/*"
    ]
  }

  statement {
    sid     = "CWLWrite"
    effect  = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "${aws_cloudwatch_log_group.fh_lg.arn}:*"
    ]
  }
}

resource "aws_iam_role_policy" "firehose_inline" {
  name   = "${var.project}-firehose-inline"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose_policy.json
}

############################################
# Kinesis Firehose Delivery Stream (Direct PUT -> S3)
############################################
resource "aws_kinesis_firehose_delivery_stream" "this" {
  name        = var.firehose_name
  destination = "extended_s3" # use Extended S3 features

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose.arn
    bucket_arn         = aws_s3_bucket.dst.arn
    buffering_size     = var.buffer_size_mb
    buffering_interval = var.buffer_interval_s

    # Keep data as JSON, GZIP-compressed (simple & cheap).
    compression_format = "GZIP"

    # Partition by event time using Firehose timestamp macros
    prefix              = "orders/event_date=!{timestamp:yyyy}-!{timestamp:MM}-!{timestamp:dd}/"
    error_output_prefix = "errors/orders/!{firehose:error-output-type}/event_date=!{timestamp:yyyy}-!{timestamp:MM}-!{timestamp:dd}/"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.fh_lg.name
      log_stream_name = aws_cloudwatch_log_stream.fh_ls.name
    }

    # If you need transformations (e.g., enrich/clean), add a Lambda processor here.
    # processing_configuration {
    #   enabled = true
    #   processors {
    #     type = "Lambda"
    #     parameters {
    #       parameter_name  = "LambdaArn"
    #       parameter_value = aws_lambda_function.transform.arn
    #     }
    #   }
    # }
  }

  tags = {
    Project = var.project
  }
}
