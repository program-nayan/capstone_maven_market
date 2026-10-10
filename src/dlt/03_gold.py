# Databricks notebook source
import dlt
from pyspark.sql.functions import col, sum as _sum, count, avg, max as _max, to_date
from src.utils.dlt_loader import load_config

# Load runtime parameters
cfg = load_config()
bronze_schema = cfg["targets"]["schemas"]["bronze"]
silver_schema = cfg["targets"]["schemas"]["silver"]
gold_schema = cfg["targets"]["schemas"]["gold"]

# ==================================================================
# FACT TABLES
# ==================================================================

# 1. Fact Sales Table
@dlt.table(
    name=f"{gold_schema}.fact_sales",
    comment="Star Schema Fact Table: Aggregated Daily Sales Performance",
    table_properties={"quality": "gold"}
)
def fact_sales():
    orders_df = dlt.read(f"{silver_schema}.silver_orders")
    
    return (
        orders_df
        .groupBy(
            "store_id",
            "product_id",
            "customer_id",
            to_date(col("order_date")).alias("order_date")
        )
        .agg(
            _sum("total_amount").alias("daily_revenue"),
            _sum("quantity").alias("total_units_sold"),
            count("order_id").alias("order_count"),
            avg("unit_price").alias("avg_unit_price")
        )
    )

# 2. Fact Inventory Table
@dlt.table(
    name=f"{gold_schema}.fact_inventory",
    comment="Star Schema Fact Table: Current Store Inventory State & Reorder Alerts",
    table_properties={"quality": "gold"}
)
def fact_inventory():
    inventory_df = dlt.read(f"{silver_schema}.silver_inventory")
    
    return (
        inventory_df
        .groupBy("store_id", "product_id")
        .agg(
            _sum("stock_on_hand").alias("current_stock"),
            _max("reorder_level").alias("reorder_threshold")
        )
        .withColumn("is_low_stock", col("current_stock") <= col("reorder_threshold"))
    )

# ==================================================================
# DIMENSION TABLES
# ==================================================================

# 1. Active Customer Dimension (Extracted from SCD Type 2)
@dlt.table(
    name=f"{gold_schema}.dim_customers",
    comment="Star Schema Dimension Table: Active Customer Profiles",
    table_properties={"quality": "gold"}
)
def dim_customers():
    scd_customers = dlt.read(f"{silver_schema}.silver_dim_customers_scd")
    
    # Filter for active state where __END_AT IS NULL
    return (
        scd_customers
        .filter(col("__END_AT").isNull())
        .withColumn("effective_from", col("__START_AT"))
    )

# 2. Store Dimension
@dlt.table(
    name=f"{gold_schema}.dim_stores",
    comment="Star Schema Dimension Table: Store Location and Hierarchy",
    table_properties={"quality": "gold"}
)
def dim_stores():
    scd_stores = dlt.read(f"{silver_schema}.silver_dim_stores_scd")
    return (
        scd_stores
        .filter(col("__END_AT").isNull())
        .withColumn("effective_from", col("__START_AT"))
    )

# 3. Product Dimension
@dlt.table(
    name=f"{gold_schema}.dim_products",
    comment="Star Schema Dimension Table: Product Catalog Master",
    table_properties={"quality": "gold"}
)
def dim_products():
    scd_products = dlt.read(f"{silver_schema}.silver_dim_products_scd")
    return (
        scd_products
        .filter(col("__END_AT").isNull())
        .withColumn("effective_from", col("__START_AT"))
    )

# 4. Calendar Dimension
@dlt.table(
    name=f"{gold_schema}.dim_calendar",
    comment="Star Schema Dimension Table: Standardized Date Hierarchy",
    table_properties={"quality": "gold"}
)
def dim_calendar():
    return dlt.read(f"{bronze_schema}.bronze_calendar")