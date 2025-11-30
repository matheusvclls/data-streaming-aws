# AWS Data Streaming Pipeline

A comprehensive data streaming pipeline built on AWS using Kinesis Firehose, S3, and AWS Glue for processing e-commerce order data. This project demonstrates real-time data ingestion, storage, and batch processing using modern AWS services.

## Architecture Overview

This pipeline consists of:
- **Data Producer**: Python script that generates synthetic e-commerce order data
- **Kinesis Firehose**: Real-time data ingestion service
- **S3 Storage**: Data lake with multiple zones (landing, raw, processed)
- **AWS Glue**: Serverless ETL jobs for data processing
- **Terraform**: Infrastructure as Code for AWS resource provisioning

## Prerequisites

1. **Install Terraform via Homebrew**
   ```bash
   brew install terraform
   ```

2. **Create AWS Account**
   - Sign up for an AWS account at [aws.amazon.com](https://aws.amazon.com)
   - Ensure you have appropriate permissions for S3, Kinesis Firehose, Glue, and IAM

3. **Configure AWS Credentials**
   ```bash
   aws configure
   ```
   Enter your AWS Access Key ID, Secret Access Key, and preferred region (default: us-east-1)

4. **Create Python Virtual Environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

## Infrastructure Setup

Navigate to the `resources` directory and run the following Terraform commands:

```bash
cd resources
terraform init
terraform validate
terraform plan
terraform apply
```

This will create:
- 5 S3 buckets (landing-zone, raw-zone, processed-zone, queryresults, resources)
- Kinesis Firehose delivery stream
- 3 AWS Glue jobs for data processing
- IAM roles and policies
- CloudWatch log groups

## Project Structure

```
├── producer_orders.py                          # Data generator script
├── pyspark-scripts/
│   ├── gluejob-raw-orders.py                  # Raw data processing job
│   ├── gluejob-processed-orders.py            # Processed data ETL job
│   └── gluejob-processed-orders-initial-load.py # Initial load job for processed data
├── resources/                                  # Terraform infrastructure
│   ├── bucket.tf                              # S3 bucket definitions
│   ├── kinesis-firehose.tf                    # Firehose configuration
│   ├── gluejob.tf                             # Glue job definitions
│   └── provider.tf                            # AWS provider configuration
├── tests/                                     # Test scripts
└── requirements.txt                           # Python dependencies
```

## Usage

### 1. Generate and Stream Data

Run the data producer to generate synthetic order data:

```bash
python producer_orders.py
```

The script will:
- Generate realistic e-commerce order data with insert/update operations
- Stream data to Kinesis Firehose in batches
- Support configurable parameters via environment variables

### 2. Monitor Data Flow

- **S3 Landing Zone**: Raw data from Firehose
- **CloudWatch Logs**: Monitor Firehose and Glue job execution
- **AWS Glue Console**: Track ETL job progress

### 3. Run ETL Jobs

Execute the Glue jobs manually or set up triggers:
- `gluejob-raw-orders`: Processes raw data from landing zone
- `gluejob-processed-orders`: Performs incremental transformations on processed data
- `gluejob-processed-orders-initial-load`: Performs initial bulk load from raw to processed zone with deduplication (run once to bootstrap the processed table)

## Configuration

### Environment Variables

- `FIREHOSE_STREAM`: Kinesis Firehose stream name (default: "firehose-orders")
- `AWS_REGION`: AWS region (default: "us-east-1")
- `TOTAL`: Total number of records to generate (default: 10000)
- `BATCH`: Batch size for data streaming (default: 100)
- `P_UPDATE`: Probability of generating update operations (default: 0.30)

### Terraform Variables

Key variables in `resources/`:
- `aws_region`: AWS region for deployment
- `bucket_name*`: S3 bucket names
- `firehose_name`: Kinesis Firehose stream name
- `glue_job_name`: Glue job names

## Data Schema

The order data includes:
- `order_id`: Unique order identifier
- `user_id`: Customer identifier
- `product_id`: Product identifier
- `database_operation`: 'i' for insert, 'u' for update
- `amount`: Order amount
- `payment_method`: Payment type (credit_card, debit_card, pix, boleto, paypal)
- `event_timestamp`: ISO timestamp

## Cleanup

To destroy all AWS resources:

```bash
cd resources
terraform destroy
```

## Dependencies

- Python 3.8+
- Terraform 1.0+
- AWS CLI configured
- boto3, Faker (see requirements.txt)

## Cost Optimization

- S3 storage classes for different data access patterns
- Glue job auto-scaling
- Firehose buffering configuration
- CloudWatch log retention policies
