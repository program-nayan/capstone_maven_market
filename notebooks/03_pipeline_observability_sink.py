# Databricks notebook source
# COMMAND ----------
import sys
import os
sys.path.append(os.path.abspath(".."))

from src.utils.config_loader import ConfigLoader
from src.telemetry.audit_views import AuditViewsManager

# COMMAND ----------
# Initialize configuration and view manager
config = ConfigLoader(env="dev")
views_manager = AuditViewsManager(spark=spark, config_loader=config)

# Deploy diagnostic views into Unity Catalog audit schema
views_manager.deploy_all_views()
print("Telemetry views deployed successfully.")

# COMMAND ----------
# Query Health Summary View
display(spark.sql("""
    SELECT * 
    FROM maven_market_uc.audit.vw_pipeline_health_summary
    ORDER BY last_execution_utc DESC
"""))

# COMMAND ----------
# Query Active Error Tracebacks View
display(spark.sql("""
    SELECT * 
    FROM maven_market_uc.audit.vw_active_error_tracebacks
    LIMIT 20
"""))