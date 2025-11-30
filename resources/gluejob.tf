variable "glue_job_name" { default = "gluejob-raw-orders" }
variable "glue_job_role_name" { default = "gluejob-raw-role" }

############################################
# IAM Role for Glue Job
############################################
data "aws_iam_policy_document" "assume_glue" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = var.glue_job_role_name
  assume_role_policy = data.aws_iam_policy_document.assume_glue.json
  tags = {
    Project = var.project
  }
}

# Attach AWS managed policy for Glue service role
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for S3 access to all required buckets
data "aws_iam_policy_document" "glue_s3_policy" {
  statement {
    sid     = "S3Access"
    effect  = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.dst.arn,
      "${aws_s3_bucket.dst.arn}/*",
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
      aws_s3_bucket.processed.arn,
      "${aws_s3_bucket.processed.arn}/*",
      aws_s3_bucket.queryresults.arn,
      "${aws_s3_bucket.queryresults.arn}/*",
      aws_s3_bucket.resources.arn,
      "${aws_s3_bucket.resources.arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name   = "${var.project}-glue-s3-access"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_s3_policy.json
}

# Policy for Glue Catalog access
data "aws_iam_policy_document" "glue_catalog_policy" {
  statement {
    sid     = "GlueCatalogAccess"
    effect  = "Allow"
    actions = [
      "glue:CreateDatabase",
      "glue:GetDatabase",
      "glue:UpdateDatabase",
      "glue:DeleteDatabase",
      "glue:CreateTable",
      "glue:GetTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTables",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:GetPartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:GetUserDefinedFunctions",
      "glue:SearchTables"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_catalog_access" {
  name   = "${var.project}-glue-catalog-access"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_catalog_policy.json
}

############################################
# Glue Job
############################################
resource "aws_glue_job" "raw_orders" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 0

  command {
    script_location = "s3://${aws_s3_bucket.resources.bucket}/pyspark-scripts/gluejob-raw-orders.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--job-bookmark-option"             = "job-bookmark-enable"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.queryresults.bucket}/spark-logs/"
    "--TempDir"                         = "s3://${aws_s3_bucket.queryresults.bucket}/temp/"
    "--enable-glue-datacatalog"         = "true"
    "--additional-python-modules"       = "pyiceberg==0.6.0"
  }

  tags = {
    Project = var.project
  }
}

############################################
# CloudWatch Log Group for Glue Job
############################################
resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = 14
  tags = {
    Project = var.project
  }
}

############################################
# Glue Job for Processed Orders
############################################
resource "aws_glue_job" "processed_orders" {
  name              = "gluejob-processed-orders"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 0

  command {
    script_location = "s3://${aws_s3_bucket.resources.bucket}/pyspark-scripts/gluejob-processed-orders.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--job-bookmark-option"             = "job-bookmark-enable"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.queryresults.bucket}/spark-logs/"
    "--TempDir"                         = "s3://${aws_s3_bucket.queryresults.bucket}/temp/"
    "--enable-glue-datacatalog"         = "true"
    "--additional-python-modules"       = "pyiceberg==0.6.0"
  }

  tags = {
    Project = var.project
  }
}

############################################
# CloudWatch Log Group for Processed Glue Job
############################################
resource "aws_cloudwatch_log_group" "glue_job_processed" {
  name              = "/aws-glue/jobs/gluejob-processed-orders"
  retention_in_days = 14
  tags = {
    Project = var.project
  }
}

############################################
# Glue Job for Initial Load (Processed Orders)
############################################
resource "aws_glue_job" "processed_orders_initial_load" {
  name              = "gluejob-processed-orders-initial-load"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 0

  command {
    script_location = "s3://${aws_s3_bucket.resources.bucket}/pyspark-scripts/gluejob-processed-orders-initial-load.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.queryresults.bucket}/spark-logs/"
    "--TempDir"                         = "s3://${aws_s3_bucket.queryresults.bucket}/temp/"
    "--enable-glue-datacatalog"         = "true"
    "--additional-python-modules"       = "pyiceberg==0.6.0"
  }

  tags = {
    Project = var.project
  }
}

############################################
# CloudWatch Log Group for Initial Load Glue Job
############################################
resource "aws_cloudwatch_log_group" "glue_job_processed_initial_load" {
  name              = "/aws-glue/jobs/gluejob-processed-orders-initial-load"
  retention_in_days = 14
  tags = {
    Project = var.project
  }
}

############################################
# S3 bucket for Glue scripts
############################################
resource "aws_s3_bucket_object" "glue_script" {
  bucket = aws_s3_bucket.resources.bucket
  key    = "pyspark-scripts/gluejob-raw-orders.py"
  source = "../pyspark-scripts/gluejob-raw-orders.py"
  etag   = filemd5("../pyspark-scripts/gluejob-raw-orders.py")
}

resource "aws_s3_bucket_object" "glue_script_processed" {
  bucket = aws_s3_bucket.resources.bucket
  key    = "pyspark-scripts/gluejob-processed-orders.py"
  source = "../pyspark-scripts/gluejob-processed-orders.py"
  etag   = filemd5("../pyspark-scripts/gluejob-processed-orders.py")
}

resource "aws_s3_bucket_object" "glue_script_processed_initial_load" {
  bucket = aws_s3_bucket.resources.bucket
  key    = "pyspark-scripts/gluejob-processed-orders-initial-load.py"
  source = "../pyspark-scripts/gluejob-processed-orders-initial-load.py"
  etag   = filemd5("../pyspark-scripts/gluejob-processed-orders-initial-load.py")
}
