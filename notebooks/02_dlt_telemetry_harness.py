# Databricks notebook source
# COMMAND ----------
import sys
import os
sys.path.append(os.path.abspath(".."))

from src.utils.config_loader import ConfigLoader
from src.telemetry.dlt_listener import DLTEventListener

# COMMAND ----------
# Initialize configuration and DLT listener
config = ConfigLoader(env="dev")
listener = DLTEventListener(spark=spark, config_loader=config, env="dev")

# COMMAND ----------
# Test listener execution against a pipeline ID
sample_pipeline_id = "00000000-0000-0000-0000-000000000000"
print(f"Testing DLT Event Listener for pipeline: {sample_pipeline_id}")

events_processed = listener.capture_pipeline_metrics(pipeline_id=sample_pipeline_id)
print(f"Total events captured and logged to audit: {events_processed}")

# COMMAND ----------
# Verify audit logs table contents
display(spark.sql("""
    SELECT * 
    FROM maven_market_uc.audit.audit_logs 
    WHERE pipeline_name LIKE 'dlt_pipeline%' 
    ORDER BY timestamp DESC 
    LIMIT 20
"""))