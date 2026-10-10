from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from src.utils.config_loader import ConfigLoader
from src.telemetry.decorators import log_execution


@log_execution(pipeline_name="bronze_batch_ingestion", step_name="auto_loader_csv_ingest")
def run_batch_csv_ingestion(spark: SparkSession, config: ConfigLoader):
    datasets = {
        "transactions_path": "transactions",
        "stores_path": "stores",
        "regions_path": "regions",
        "calendar_path": "calendar",
        "returns_path": "returns"
    }

    for source_key, table_name in datasets.items():
        source_path = config.get_raw_csv_path(source_key)
        target_table = config.get_full_table_name("bronze", table_name)
        checkpoint_path = config.get_checkpoint_path(f"autoloader_{table_name}")

        print(f"[{config.environment.upper()}] Starting Auto Loader for {table_name}...")

        df_stream = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
            .load(source_path)
        )

        df_enriched = (
            df_stream
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file_path", col("_metadata.file_path"))
            .withColumn("_source_file_name", col("_metadata.file_name")) 
        )

        query = (
            df_enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .toTable(target_table)
        )

        query.awaitTermination()
        print(f"Successfully ingested {table_name} into {target_table}\n")


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    cfg = ConfigLoader("config/config.yml")
    run_batch_csv_ingestion(spark_session, cfg)