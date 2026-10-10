# Databricks notebook source
# COMMAND ----------
import sys
import os
sys.path.append(os.path.abspath(".."))

from src.utils.config_loader import ConfigLoader
from src.telemetry.dlt_listener import capture_dlt_pipeline_events

# COMMAND ----------
# Load configuration and fetch pipeline ID dynamically
config = ConfigLoader(env="dev")
pipeline_id = config.get_dlt_pipeline_id()

if not pipeline_id:
    raise ValueError("DLT pipeline_id is missing from config.yml or environment variables.")

print(f"Executing DLT Telemetry Listener for Pipeline ID: {pipeline_id}...")

# Capture DLT execution events and log to audit.audit_logs
capture_dlt_pipeline_events(
    spark=spark,
    pipeline_id=pipeline_id,
    catalog=config.get_catalog(),
    environment=config.environment
)

print("DLT event metrics successfully processed and logged to audit table.")

# COMMAND ----------
# Query audit log table to verify ingested DLT events
display(spark.sql("""
    SELECT * 
    FROM maven_market_uc.audit.audit_logs 
    WHERE pipeline_name LIKE 'dlt_pipeline%' 
    ORDER BY timestamp DESC 
    LIMIT 20
"""))