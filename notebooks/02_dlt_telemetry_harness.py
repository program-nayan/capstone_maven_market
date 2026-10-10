# Databricks notebook source
# COMMAND ----------
import sys
import os
sys.path.append(os.path.abspath(".."))

from src.telemetry.dlt_listener import capture_dlt_pipeline_events

# COMMAND ----------
# Paste your DLT Pipeline ID (from the DLT UI pipeline details URL)
sample_pipeline_id = "<YOUR_DLT_PIPELINE_ID>"

print(f"Executing DLT Telemetry Listener for Pipeline ID: {sample_pipeline_id}...")

# Execute event parser to log metrics to maven_market_uc.audit.audit_logs
capture_dlt_pipeline_events(
    spark=spark,
    pipeline_id=sample_pipeline_id,
    catalog="maven_market_uc",
    environment="dev"
)

print("DLT event metrics successfully processed and logged to audit table.")

# COMMAND ----------
# Verify ingested event logs in audit table
display(spark.sql("""
    SELECT * 
    FROM maven_market_uc.audit.audit_logs 
    WHERE pipeline_name LIKE 'dlt_pipeline%' 
    ORDER BY timestamp DESC 
    LIMIT 20
"""))