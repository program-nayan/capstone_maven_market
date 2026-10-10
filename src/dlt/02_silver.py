# Databricks notebook source
import dlt
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp, to_date
from src.utils.dlt_loader import load_config

# Load runtime parameters
cfg = load_config()
catalog = cfg["catalog"]
bronze_schema = cfg["targets"]["schemas"]["bronze"]
silver_schema = cfg["targets"]["schemas"]["silver"]
expectations = cfg["expectations"]

# ------------------------------------------------------------------
# 1. Cleansed Store Master Data
# ------------------------------------------------------------------
@dlt.table(
    name=f"{silver_schema}.silver_stores",
    comment="Cleansed store master combined with regional details",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_store_id", expectations["stores"]["valid_store_id"])
def silver_stores():
    stores_df = dlt.read(f"{bronze_schema}.bronze_stores")
    regions_df = dlt.read(f"{bronze_schema}.bronze_regions").drop(
        "_ingested_at", "_source_file", "_source_file_name", "_rescued_data"
    )
    
    return stores_df.join(regions_df, on="region_id", how="left")

# ------------------------------------------------------------------
# 2. Cleansed Product Catalog
# ------------------------------------------------------------------
@dlt.table(
    name=f"{silver_schema}.silver_products",
    comment="Cleansed MongoDB product catalog data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_product_id", expectations["products"]["valid_product_id"])
@dlt.expect_or_drop("valid_price", expectations["products"]["valid_price"])
def silver_products():
    return dlt.read(f"{bronze_schema}.bronze_mongodb_products")

# ------------------------------------------------------------------
# 3. Stream-Static Join: Kafka Orders Stream + MongoDB Product Master
# ------------------------------------------------------------------
@dlt.table(
    name=f"{silver_schema}.silver_orders",
    comment="Cleansed order events joined with product attributes",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_quantity", expectations["orders"]["valid_quantity"])
@dlt.expect_or_drop("valid_customer_id", expectations["orders"]["valid_customer_id"])
def silver_orders():
    # 1. Stream Kafka order messages
    kafka_stream = dlt.read_stream(f"{bronze_schema}.bronze_kafka_orders")
    
    # 2. Static Product reference data
    products_df = dlt.read(f"{silver_schema}.silver_products")
    
    # 3. Kafka Payload Schema Parsing
    order_payload_schema = (
        "order_id STRING, transaction_date TIMESTAMP, stock_date STRING, "
        "product_id STRING, customer_id STRING, store_id STRING, "
        "quantity INT, unit_price DOUBLE, event_timestamp TIMESTAMP"
    )
    
    parsed_orders = (
        kafka_stream
        .select(
            from_json(col("value").cast("string"), order_payload_schema).alias("payload"),
            col("_ingested_at")
        )
        .select("payload.*", "_ingested_at")
    )
    
    # 4. Stream-Static Join
    return (
        parsed_orders
        .join(products_df, on="product_id", how="inner")
        .select(
            parsed_orders["order_id"],
            parsed_orders["customer_id"],
            parsed_orders["store_id"],
            parsed_orders["product_id"],
            parsed_orders["quantity"],
            parsed_orders["unit_price"],
            (parsed_orders["quantity"] * parsed_orders["unit_price"]).alias("total_amount"),
            parsed_orders["transaction_date"].alias("order_date"),
            products_df["product_name"],
            products_df["product_brand"],
            products_df["product_retail_price"]
        )
    )

# ------------------------------------------------------------------
# 4. Cleansed Real-Time Inventory Stream
# ------------------------------------------------------------------
@dlt.table(
    name=f"{silver_schema}.silver_inventory",
    comment="Parsed streaming inventory level updates",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_stock_count", "stock_on_hand IS NOT NULL AND stock_on_hand >= 0")
def silver_inventory():
    kafka_inventory_stream = dlt.read_stream(f"{bronze_schema}.bronze_kafka_inventory")
    
    # 1. Exact schema matching the raw Kafka JSON payload
    inventory_payload_schema = (
        "event_id STRING, store_id STRING, product_id STRING, "
        "stock_level INT, reorder_threshold INT, restock_flag BOOLEAN, "
        "event_timestamp TIMESTAMP"
    )
    
    # 2. Parse JSON and alias keys to Silver column standards
    return (
        kafka_inventory_stream
        .select(
            from_json(col("value").cast("string"), inventory_payload_schema).alias("data"),
            col("_ingested_at")
        )
        .select(
            col("data.event_id"),
            col("data.store_id"),
            col("data.product_id"),
            col("data.stock_level").alias("stock_on_hand"),
            col("data.reorder_threshold").alias("reorder_level"),
            col("data.restock_flag"),
            col("data.event_timestamp").alias("last_updated"),
            col("_ingested_at")
        )
    )

# ------------------------------------------------------------------
# 5. Customer Master SCD Type 2 History Tracking
# ------------------------------------------------------------------
dlt.create_streaming_table(
    name=f"{silver_schema}.silver_dim_customers_scd",
    comment="SCD Type 2 history tracking table for MongoDB customer records",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target=f"{silver_schema}.silver_dim_customers_scd",
    source=f"{bronze_schema}.bronze_mongodb_customers",
    keys=["customer_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=2,
    track_history_column_list=["customer_city", "customer_state_province", "customer_postal_code", "customer_country", "marital_status", "yearly_income", "total_children", "education"]
)

# ------------------------------------------------------------------
# 6. Product Catalog SCD Type 2 History Tracking
# ------------------------------------------------------------------
dlt.create_streaming_table(
    name=f"{silver_schema}.silver_dim_products_scd",
    comment="SCD Type 2 history tracking table for MongoDB product catalog",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target=f"{silver_schema}.silver_dim_products_scd",
    source=f"{bronze_schema}.bronze_mongodb_products",
    keys=["product_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=2,
    track_history_column_list=[
        "product_retail_price",
        "product_cost",
        "product_brand"
    ]
)

# ------------------------------------------------------------------
# 7. Store Master SCD Type 2 History Tracking
# ------------------------------------------------------------------
dlt.create_streaming_table(
    name=f"{silver_schema}.silver_dim_stores_scd",
    comment="SCD Type 2 history tracking table for POS store master data",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target=f"{silver_schema}.silver_dim_stores_scd",
    source=f"{bronze_schema}.bronze_stores",
    keys=["store_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=2,
    track_history_column_list=[
        "store_name",
        "region_id",
        "store_city",
        "store_state",
        "store_phone"
    ]
)
