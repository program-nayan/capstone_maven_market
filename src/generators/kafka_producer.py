import time
import json
import random
import pymongo
from datetime import datetime, timedelta
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils
from src.utils.config_loader import ConfigLoader

def load_reference_dimension_keys_pyspark(spark: SparkSession, config: ConfigLoader, secret_scope: str = "scope-maven-market"):
    base_storage_path = config._config["storage"]["base_path"]
    
    # 1. Read Store IDs from ADLS Gen2 CSV
    stores_path = f"{base_storage_path}{config._config['sources']['pos_csv']['stores']}"
    valid_store_ids = [
        str(row[0]).strip() 
        for row in spark.read.option("header", "true").csv(stores_path).select("store_id").distinct().collect()
        if row[0] is not None
    ]

    # 2. Connect to MongoDB Atlas
    mongo_uri = dbutils.secrets.get(scope=secret_scope, key="mongodb-connection-uri").strip()
    mongo_cfg = config._config["sources"]["mongodb"]
    db_name = mongo_cfg.get("database", "maven_market_db")
    
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]

    # 3. Read Customer IDs from MongoDB Atlas
    raw_customers = list(db[mongo_cfg["collections"]["customers"]].find({}, {"customer_id": 1, "id": 1, "_id": 1}))
    valid_customer_ids = [
        str(doc.get("customer_id") or doc.get("id") or doc.get("_id")).strip()
        for doc in raw_customers
        if (doc.get("customer_id") or doc.get("id") or doc.get("_id")) is not None
    ]

    # 4. Read Product Catalog from MongoDB Atlas
    raw_products = list(db[mongo_cfg["collections"]["products"]].find({}, {"product_id": 1, "id": 1, "_id": 1, "product_retail_price": 1}))
    valid_products = [
        {
            "product_id": str(doc.get("product_id") or doc.get("id") or doc.get("_id")).strip(),
            "unit_price": float(doc.get("product_retail_price") or 2.50)
        }
        for doc in raw_products
        if (doc.get("product_id") or doc.get("id") or doc.get("_id")) is not None
    ]

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