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
checkpoint_path = "s3://processed-zone-622813843927/checkpoints/orders/"

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
# Leitura streaming dos dados da tabela raw.orders
# ---------------------------
df_stream = (
    spark.readStream
    .format("iceberg")
    .table(f"glue_catalog.{source_database}.{source_table}")
)

# ---------------------------
# Função para MERGE em tabela
# ---------------------------
def merge_to_existing_table(microbatch, batch_id):
    """Faz MERGE em uma tabela que já tem dados com deduplicação"""
    try:
        print(f"Starting MERGE operation for batch {batch_id}...")
        
        # Deduplicação do micro-lote antes do MERGE
        ts_col = "event_timestamp"
        key_col = "order_id"
        
        if ts_col in microbatch.columns and key_col in microbatch.columns:
            w = Window.partitionBy(F.col(key_col)).orderBy(F.col(ts_col).desc())
            microbatch = microbatch.withColumn("row_rank", F.row_number().over(w)).filter("row_rank = 1").drop("row_rank")
            print(f"Deduplication applied for batch {batch_id}")
        
        # Cria uma tabela temporária no S3 para o MERGE
        temp_table_name = f"temp_updates_{batch_id}"
        temp_path = f"s3://processed-zone-622813843927/temp/{temp_table_name}/"
        
        # Salva o microbatch como uma tabela temporária Iceberg
        microbatch.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("write.format.default", "parquet") \
            .saveAsTable(f"glue_catalog.{target_database}.{temp_table_name}")
        
        print(f"Temporary table {temp_table_name} created...")
        
        spark.sql(f"""
            MERGE INTO glue_catalog.{target_database}.{target_table} AS t
            USING glue_catalog.{target_database}.{temp_table_name} AS u
            ON t.order_id = u.order_id
            WHEN MATCHED THEN UPDATE SET
                t.user_id = u.user_id,
                t.product_id = u.product_id,
                t.database_operation = u.database_operation,
                t.amount = u.amount,
                t.payment_method = u.payment_method,
                t.event_date = u.event_date,
                t.event_timestamp = u.event_timestamp,
                t.etl_timestamp = u.etl_timestamp,
                t.source_filename = u.source_filename
            WHEN NOT MATCHED THEN INSERT (
                order_id, user_id, product_id, database_operation, amount, 
                payment_method, event_date, event_timestamp, etl_timestamp, source_filename
            )
            VALUES (
                u.order_id, u.user_id, u.product_id, u.database_operation, u.amount,
                u.payment_method, u.event_date, u.event_timestamp, u.etl_timestamp, u.source_filename
            )
        """)
        
        # Limpa a tabela temporária
        spark.sql(f"DROP TABLE IF EXISTS glue_catalog.{target_database}.{temp_table_name}")
        print(f"Temporary table {temp_table_name} cleaned up...")
        
        print(f"MERGE completed successfully for batch {batch_id}")
    except Exception as e:
        print(f"Error in MERGE: {str(e)}")
        raise e

# ---------------------------
# Função principal de upsert por micro-lote
# ---------------------------
def upsert_to_iceberg(microbatch, batch_id):
    """
    Função para processar cada micro-lote e fazer MERGE na tabela processed.orders
    """
    print(f"Processing batch {batch_id} with {microbatch.count()} records")
    
    # Sempre usa MERGE (a carga inicial deve ser feita pelo script de initial-load)
    merge_to_existing_table(microbatch, batch_id)
    
    print(f"Batch {batch_id} processed successfully")

# ---------------------------
# Inicia o stream usando foreachBatch
# ---------------------------
query = (
    df_stream.writeStream
    .option("checkpointLocation", checkpoint_path)
    .outputMode("update")   # 'update' para permitir upserts
    .foreachBatch(upsert_to_iceberg)
    .start()
)

print("Streaming query started. Processing orders from raw to processed...")
print(f"Source: glue_catalog.{source_database}.{source_table}")
print(f"Target: glue_catalog.{target_database}.{target_table}")
print(f"Checkpoint: {checkpoint_path}")

# Aguarda o streaming
query.awaitTermination()
