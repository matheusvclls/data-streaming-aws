import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

# ---- Parâmetros ----
source_database = "raw"
source_table = "orders"
target_database = "processed"
target_table = "orders"
warehouse_path = "s3://processed-zone-622813843927/iceberg/"

def qid(name: str) -> str:
    """Quote de identificadores SQL com backticks, escapando crases internas."""
    return f"`{name.replace('`', '``')}`"

# ---------------------------
# Spark/Glue com Iceberg (Glue Catalog)
# ---------------------------
sc = SparkContext.getOrCreate()
glue_ctx = GlueContext(sc)
spark: SparkSession = (
    glue_ctx.spark_session.builder
    # Iceberg + Glue Catalog
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "glue_catalog")
    .config("spark.sql.warehouse.dir", warehouse_path)
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", warehouse_path)
    # Integração Glue/Hive
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .getOrCreate()
)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Use explicitamente o catálogo/DB corretos
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{qid(target_database)}")

# ---------------------------
# Cria a tabela Iceberg de destino (processed.orders), se não existir
# ---------------------------
spark.sql(f"""
  CREATE EXTERNAL TABLE IF NOT EXISTS glue_catalog.{target_database}.{target_table} (
    order_id            STRING,
    user_id             STRING,
    product_id          STRING,
    database_operation  STRING,
    amount              DECIMAL(12,2),
    payment_method      STRING,
    event_date          DATE,
    event_timestamp     TIMESTAMP,
    etl_timestamp       TIMESTAMP,
    source_filename     STRING
  )
  USING iceberg
  LOCATION 's3://processed-zone-622813843927/iceberg/orders/'
  PARTITIONED BY (days(event_date))
""")

# ---------------------------
# Verifica se a tabela de destino já tem dados
# ---------------------------
target_table_name = f"glue_catalog.{target_database}.{target_table}"
try:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table_name}").collect()[0]["cnt"]
    if count > 0:
        print(f"WARNING: Target table {target_table_name} already has {count} records.")
        print("This script is for initial load only. Exiting to avoid duplicates.")
        sys.exit(0)
except Exception as e:
    print(f"Table might be empty or not accessible: {e}")
    print("Proceeding with initial load...")

# ---------------------------
# Leitura batch dos dados da tabela raw.orders
# ---------------------------
print(f"Reading data from glue_catalog.{source_database}.{source_table}...")
df_raw = spark.read.format("iceberg").table(f"glue_catalog.{source_database}.{source_table}")

raw_count = df_raw.count()
print(f"Total records read from raw table: {raw_count}")

if raw_count == 0:
    print("No data found in raw table. Exiting.")
    sys.exit(0)

# ---------------------------
# Deduplicação dos dados
# ---------------------------
print("Applying deduplication...")
ts_col = "event_timestamp"
key_col = "order_id"

if ts_col in df_raw.columns and key_col in df_raw.columns:
    # Mantém apenas o registro mais recente para cada order_id
    w = Window.partitionBy(F.col(key_col)).orderBy(F.col(ts_col).desc())
    df_dedup = df_raw.withColumn("row_rank", F.row_number().over(w)).filter("row_rank = 1").drop("row_rank")
    
    dedup_count = df_dedup.count()
    print(f"Records after deduplication: {dedup_count} (removed {raw_count - dedup_count} duplicates)")
else:
    print(f"WARNING: Columns {ts_col} or {key_col} not found. Skipping deduplication.")
    df_dedup = df_raw

# ---------------------------
# Inserção na tabela processed.orders
# ---------------------------
print(f"Inserting {df_dedup.count()} records into {target_table_name}...")

df_dedup.write \
    .format("iceberg") \
    .mode("append") \
    .option("write.format.default", "parquet") \
    .saveAsTable(target_table_name)

# Verifica o resultado
final_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table_name}").collect()[0]["cnt"]
print(f"Initial load completed successfully!")
print(f"Total records in {target_table_name}: {final_count}")

