import json
import pymongo
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from databricks.sdk.runtime import dbutils
from src.utils.config_loader import ConfigLoader

def run_mongodb_ingestion(spark: SparkSession, config: ConfigLoader, secret_scope: str = "scope-maven-market"):
    mongo_cfg = config.get_mongodb_config()
    collections_map = config.get_mongodb_collections()
    db_name = mongo_cfg["database"]

    mongo_uri = dbutils.secrets.get(scope=secret_scope, key="mongodb-connection-uri")
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]

    for entity_name, collection_name in collections_map.items():
        target_table = config.get_full_table_name("bronze", f"mongodb_{entity_name}")
        print(f"Connecting to MongoDB Atlas [{db_name}.{collection_name}] -> {target_table}...")

        collection = db[collection_name]
        raw_docs = list(collection.find())

        if not raw_docs:
            print(f"⚠️ No documents retrieved from collection: {collection_name}")
            continue

        # Convert BSON objects into standard JSON strings
        cleaned_docs = [json.loads(json.dumps(doc, default=str)) for doc in raw_docs]

        # Convert to Pandas DataFrame first to unify numeric types (int vs float)
        df_pd = pd.DataFrame(cleaned_docs)

        # Convert to PySpark DataFrame
        df_mongo = spark.createDataFrame(df_pd)

        # Enrich metadata
        df_enriched = (
            df_mongo
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_system", lit(f"MongoDB_Atlas.{collection_name}"))
        )

        # Write Delta table to Unity Catalog Bronze layer
        (
            df_enriched.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        print(f" Successfully ingested {len(cleaned_docs)} documents into {target_table}\n")

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    cfg = ConfigLoader("config/config.yml")
    run_mongodb_ingestion(spark_session, cfg)