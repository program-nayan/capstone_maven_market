import time
import json
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils
from src.utils.config_loader import ConfigLoader

def load_reference_dimension_keys(spark: SparkSession, config: ConfigLoader):
    """
    Fetches valid Store IDs, Customer IDs, and Product (ID, Price) pairs 
    from the ingested Bronze layer tables to guarantee referential integrity.
    """
    print("Fetching reference keys from Unity Catalog Bronze tables...")
    
    stores_table = config.get_full_table_name("bronze", "stores")
    cust_table = config.get_full_table_name("bronze", "mongodb_customers")
    prod_table = config.get_full_table_name("bronze", "mongodb_products")

    # 1. Fetch valid store IDs
    valid_store_ids = [row.store_id for row in spark.table(stores_table).select("store_id").distinct().collect()]

    # 2. Fetch valid customer IDs
    valid_customer_ids = [row.customer_id for row in spark.table(cust_table).select("customer_id").distinct().collect()]

    # 3. Fetch valid product IDs and their retail prices
    valid_products = [
        {"product_id": int(row.product_id), "unit_price": float(row.product_retail_price or 2.50)}
        for row in spark.table(prod_table).select("product_id", "product_retail_price").distinct().collect()
        if row.product_id is not None
    ]

    print(f" Loaded {len(valid_store_ids)} stores, {len(valid_customer_ids)} customers, and {len(valid_products)} products.")
    return valid_store_ids, valid_customer_ids, valid_products

def run_kafka_producer(spark: SparkSession, config: ConfigLoader, secret_scope: str = "scope-maven-market", num_events: int = 300, delay: float = 0.3):
    kafka_cfg = config.get_kafka_config()
    
    # Fetch secrets securely from Key Vault
    kafka_bootstrap = kafka_cfg.get("bootstrap_servers")
    kafka_api_key = dbutils.secrets.get(scope=secret_scope, key="kafka-api-key")
    kafka_api_secret = dbutils.secrets.get(scope=secret_scope, key="kafka-api-secret")

    # Fetch real foreign keys from Bronze tables
    store_ids, customer_ids, product_catalog = load_reference_dimension_keys(spark, config)

    producer_config = {
        'bootstrap.servers': kafka_bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': kafka_api_key,
        'sasl.password': kafka_api_secret
    }

    producer = Producer(producer_config)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")

    orders_topic = kafka_cfg["topics"]["orders"]
    inventory_topic = kafka_cfg["topics"]["inventory"]

    print(f"Streaming referentially aligned events to Confluent Kafka [{kafka_bootstrap}]...")

    for i in range(num_events):
        now_dt = datetime.utcnow()
        now_str = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        stock_date_str = (now_dt - timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%d")

        # Pick real foreign keys from ingested datasets
        selected_store = random.choice(store_ids)
        selected_customer = random.choice(customer_ids)
        selected_prod = random.choice(product_catalog)
        
        # 1. Order Event (Matches MongoDB Products, Customers, and Stores)
        order_event = {
            "order_id": f"ORD-{random.randint(100000, 999999)}",
            "transaction_date": now_str,
            "stock_date": stock_date_str,
            "product_id": selected_prod["product_id"],
            "customer_id": selected_customer,
            "store_id": selected_store,
            "quantity": random.randint(1, 6),
            "unit_price": selected_prod["unit_price"],
            "event_timestamp": now_str
        }
        producer.produce(
            topic=orders_topic,
            key=order_event["order_id"],
            value=json.dumps(order_event),
            callback=delivery_report
        )

        # 2. Inventory Event (Matches Products and Stores)
        stock_qty = random.randint(5, 120)
        reorder_thresh = 20
        inventory_event = {
            "event_id": f"INV-{random.randint(100000, 999999)}",
            "store_id": selected_store,
            "product_id": selected_prod["product_id"],
            "stock_level": stock_qty,
            "reorder_threshold": reorder_thresh,
            "restock_flag": bool(stock_qty <= reorder_thresh),
            "event_timestamp": now_str
        }
        producer.produce(
            topic=inventory_topic,
            key=f"{selected_store}-{selected_prod['product_id']}",
            value=json.dumps(inventory_event),
            callback=delivery_report
        )

        producer.poll(0)
        time.sleep(delay)

    producer.flush()
    print(f" Successfully generated and pushed {num_events} referentially valid event pairs!")

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    cfg = ConfigLoader("config/config.yml")
    run_kafka_producer(spark_session, cfg)