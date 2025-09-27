import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

# ---- Parâmetros ----
database        = "raw"
table           = "orders"
src_format      = "json"
src_path        = "s3://landing-zone-622813843927/orders/"
warehouse_path  = "s3://raw-zone-622813843927/iceberg/"
checkpoint_path = "s3://raw-zone-622813843927/checkpoints/orders/"

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
    .config("spark.sql.warehouse.dir", warehouse_path)  # evita fallback para file:/tmp
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
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{qid(database)}")

# ---------------------------
# Descobre schema em batch (obrigatório para JSON/CSV em streaming)
# ---------------------------
data_schema = spark.read.format(src_format).load(src_path).schema

# Monta colunas com quoting seguro
cols = ", ".join([f"{qid(f.name)} {f.dataType.simpleString()}" for f in data_schema.fields])

# ---------------------------
# Cria a tabela Iceberg (no catálogo correto), se não existir
# ---------------------------
spark.sql("""
  CREATE EXTERNAL TABLE IF NOT EXISTS glue_catalog.raw.orders (
    order_id            STRING,
    user_id             STRING,
    product_id          STRING,
    database_operation  STRING,          -- 'i' (insert) | 'u' (update)
    amount              DECIMAL(12,2),
    payment_method      STRING,          -- 'debit_card' | 'boleto' | 'pix' | 'credit_card' | 'paypal' ...
    etl_timestamp       TIMESTAMP,       -- preenchida no job
    event_date          DATE,            -- preenchida no job
    date_event          DATE,            -- preenchida no job
    source_filename     STRING           -- preenchida no job
  )
  USING iceberg
  LOCATION 's3://raw-zone-622813843927/iceberg/orders/'
  PARTITIONED BY (days(event_date))
""")

# ---------------------------
# Leitura streaming e enriquecimentos
# ---------------------------

df = (
    spark.readStream
         .format(src_format)
         .schema(data_schema)
         .load(src_path)
)

df = (
    df
    .withColumn("order_id", F.col("order_id").cast("string"))
    .withColumn("user_id", F.col("user_id").cast("string"))
    .withColumn("product_id", F.col("product_id").cast("string"))
    .withColumn("payment_method", F.col("payment_method").cast("string"))
    # amount -> DECIMAL(12,2). Se já vier como double com ponto decimal, pode trocar por cast direto.
    .withColumn("amount",
        F.regexp_replace(F.col("amount").cast("string"), ",", ".").cast(T.DecimalType(12, 2))
    )
    # normaliza database_operation para 'i'/'u'
    .withColumn("database_operation", F.lower(F.col("database_operation").cast("string")))
    .withColumn(
        "database_operation",
        F.when(F.col("database_operation").startswith("i"), F.lit("i"))
         .when(F.col("database_operation").startswith("u"), F.lit("u"))
         .otherwise(F.lit("i"))
    )
    .withColumn("etl_timestamp", F.current_timestamp())
    # garante que date_event exista e seja DATE
    .withColumn("date_event", F.to_date(F.col("date_event")))
    .withColumn("event_date", F.to_date(F.col("date_event")))
    .withColumn("source_filename", F.input_file_name().cast("string"))
    # reordena exatamente como no schema da tabela
    .select(
        "order_id",
        "user_id",
        "product_id",
        "database_operation",
        "amount",
        "payment_method",
        "etl_timestamp",
        "event_date",
        "date_event",
        "source_filename",
    )
)

# ---------------------------
# Escrita streaming em Iceberg (sempre com catálogo qualificado)
# ---------------------------
(
    df.writeStream
      .format("iceberg")
      .outputMode("append")
      .option("checkpointLocation", checkpoint_path)
      .toTable(f"glue_catalog.{database}.{table}")
      .awaitTermination()
)
