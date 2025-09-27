variable "bucket_name"       { default = "landing-zone-622813843927" }
variable "bucket_name_processed"       { default = "processed-zone-622813843927" }
variable "bucket_name_raw"       { default = "raw-zone-622813843927" }
variable "bucket_name_queryresults"       { default = "queryresults-zone-622813843927" }

resource "aws_s3_bucket" "dst" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket" "processed" {
  bucket = var.bucket_name_processed
}

resource "aws_s3_bucket" "queryresults" {
  bucket = var.bucket_name_queryresults
}

resource "aws_s3_bucket" "raw" {
  bucket = var.bucket_name_raw
}

resource "aws_s3_bucket_public_access_block" "dst" {
  bucket                  = aws_s3_bucket.dst.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "queryresults" {
  bucket                  = aws_s3_bucket.queryresults.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
