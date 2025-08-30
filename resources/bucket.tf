variable "bucket_name"       { default = "landing-zone-622813843927" }

resource "aws_s3_bucket" "dst" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "dst" {
  bucket                  = aws_s3_bucket.dst.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
