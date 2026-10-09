import dlt
import json
import pymongo
import pandas as pd
from pyspark.sql.functions import current_timestamp, col, input_file_name, lit 
from src.utils.dlt_loader import load_config
from databricks.sdk.runtime import dbutils

# Load runtime parameters
cfg = load_config()
base_path = cfg["storage"]["base_path"]
secret_scope = cfg["secret_scope"]
catalog = cfg["catalog"]
bronze_schema = cfg["targets"]["schemas"]["bronze"]

# ------------------------------------------------------------------
# 1. POS CSV Auto Loader Stream Ingestion
# ------------------------------------------------------------------
for dataset_key, subpath in cfg["sources"]["pos_csv"].items():
    # Strip '_path' suffix if present (e.g., 'transactions_path' -> 'transactions')
    clean_entity_name = dataset_key.replace("_path", "")
    table_name = f"bronze_{clean_entity_name}"
    full_source_path = f"{base_path}{subpath}"

    def create_csv_table(path=full_source_path, tbl=table_name):
        @dlt.table(
            name=f"{bronze_schema}.{tbl}",  # Resolves to 'bronze.bronze_transactions'
            comment=f"Raw streaming CSV ingestion for {tbl}",
            table_properties={"quality": "bronze"}
        )
        def ingest_csv():
            return (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(path)
                .withColumn("_ingested_at", current_timestamp())
                .withColumn("_source_file", col("_metadata.file_path"))
                .withColumn("_source_file_name", col("_metadata.file_name"))
            )
    create_csv_table()

# ------------------------------------------------------------------
# 2. MongoDB Staged Views from Unity Catalog
# ------------------------------------------------------------------

# MongoDB Configuration
mongo_cfg = cfg["sources"]["mongodb"]
db_name = mongo_cfg["database"]
mongo_secret_key = mongo_cfg.get("uri_secret_key", "mongodb-connection-uri")

# Retrieve MongoDB connection string securely from Azure Key Vault
mongo_uri = dbutils.secrets.get(scope=secret_scope, key=mongo_secret_key).strip()

for entity_name, collection_name in mongo_cfg["collections"].items():
    dlt_tbl_name = f"bronze_mongodb_{entity_name}"

    def create_mongo_dlt_table(coll_name, tbl):
        @dlt.table(
            name=tbl,  # Registers 'bronze_mongodb_customers' & 'bronze_mongodb_products'
            comment=f"Raw MongoDB Atlas direct ingestion for {coll_name} via PyMongo",
            table_properties={"quality": "bronze"}
        )
        def ingest_mongo():
            # 1. Fetch raw documents from MongoDB Atlas via PyMongo
            client = pymongo.MongoClient(mongo_uri)
            db = client[db_name]
            collection = db[coll_name]
            
            raw_docs = list(collection.find())
            print(raw_docs)

            if not raw_docs:
                # Return empty DataFrame with dummy schema if collection is empty
                pass

            # 2. Convert BSON objects into standard JSON strings
            cleaned_docs = [json.loads(json.dumps(doc, default=str)) for doc in raw_docs]

            # 3. Convert to Pandas DataFrame first to unify numeric types
            df_pd = pd.DataFrame(cleaned_docs)

            # 4. Convert Pandas DataFrame to PySpark DataFrame
            df_mongo = spark.createDataFrame(df_pd)

            # 5. Enrich metadata and return DataFrame (DLT handles saving to Unity Catalog)
            return (
                df_mongo
                .withColumn("_ingested_at", current_timestamp())
                .withColumn("_source_system", lit(f"MongoDB_Atlas.{coll_name}"))
            )

    create_mongo_dlt_table(collection_name, dlt_tbl_name)

# ------------------------------------------------------------------
# 3. Dynamic Kafka Streams Ingestion
# ------------------------------------------------------------------
kafka_cfg = cfg["sources"]["kafka"]

def get_sanitized_kafka_creds():
    raw_bootstrap = dbutils.secrets.get(scope=secret_scope, key=kafka_cfg["bootstrap_secret_key"]).strip()
    clean_bootstrap = raw_bootstrap.replace("https://", "").replace("http://", "").rstrip("/")
    api_key = dbutils.secrets.get(scope=secret_scope, key=kafka_cfg["api_key_secret_key"]).strip()
    api_secret = dbutils.secrets.get(scope=secret_scope, key=kafka_cfg["api_secret_secret_key"]).strip()

    jaas_config = (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{api_key}" password="{api_secret}";'
    )
    return clean_bootstrap, jaas_config

@dlt.table(
    name=f"{bronze_schema}.bronze_kafka_orders",
    comment="Live real-time POS order stream from Confluent Cloud",
    table_properties={"quality": "bronze"}
)
def bronze_kafka_orders():
    bootstrap, jaas = get_sanitized_kafka_creds()
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("subscribe", kafka_cfg["topics"]["orders"])
        .option("startingOffsets", "earliest")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )

@dlt.table(
    name=f"{bronze_schema}.bronze_kafka_inventory",
    comment="Live store stock inventory stream from Confluent Cloud",
    table_properties={"quality": "bronze"}
)
def bronze_kafka_inventory():
    bootstrap, jaas = get_sanitized_kafka_creds()
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("subscribe", kafka_cfg["topics"]["inventory"])
        .option("startingOffsets", "earliest")
        .load()
        .withColumn("_ingested_at", current_timestamp())
    )