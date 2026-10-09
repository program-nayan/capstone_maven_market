from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from databricks.sdk.runtime import dbutils
from src.utils.config_loader import ConfigLoader

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("stock_date", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])

INVENTORY_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("stock_level", IntegerType(), True),
    StructField("reorder_threshold", IntegerType(), True),
    StructField("restock_flag", BooleanType(), True),
    StructField("event_timestamp", StringType(), True)
])

def run_kafka_streaming_consumer(spark: SparkSession, config: ConfigLoader, secret_scope: str = "scope-maven-market"):
    kafka_cfg = config.get_kafka_config()
    
    # Clean secrets
    raw_bootstrap = dbutils.secrets.get(scope=secret_scope, key="kafka-bootstrap-servers").strip()
    kafka_bootstrap = raw_bootstrap.replace("https://", "").replace("http://", "").rstrip("/")
    
    kafka_api_key = dbutils.secrets.get(scope=secret_scope, key="kafka-api-key").strip()
    kafka_api_secret = dbutils.secrets.get(scope=secret_scope, key="kafka-api-secret").strip()

    # Databricks runtime class prefix
    jaas_config = (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_api_key}" password="{kafka_api_secret}";'
    )

    # --- Stream 1: Orders ---
    orders_topic = kafka_cfg["topics"]["orders"]
    orders_target = config.get_full_table_name("bronze", "kafka_orders")
    orders_checkpoint = config.get_checkpoint_path("kafka_orders")

    raw_orders = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("subscribe", orders_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_orders = (
        raw_orders
        .select(from_json(col("value").cast("string"), ORDERS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("_ingested_at", current_timestamp())
    )

    query_orders = (
        parsed_orders.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", orders_checkpoint)
        .toTable(orders_target)
    )

    # --- Stream 2: Inventory ---
    inventory_topic = kafka_cfg["topics"]["inventory"]
    inventory_target = config.get_full_table_name("bronze", "kafka_inventory")
    inventory_checkpoint = config.get_checkpoint_path("kafka_inventory")

    raw_inventory = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("subscribe", inventory_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_inventory = (
        raw_inventory
        .select(from_json(col("value").cast("string"), INVENTORY_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("_ingested_at", current_timestamp())
    )

    query_inventory = (
        parsed_inventory.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", inventory_checkpoint)
        .toTable(inventory_target)
    )

    return [query_orders, query_inventory]