import sys
import traceback
from datetime import datetime, timezone
import json
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)

class TelemetryLogger:
    """Centralized structured logging module for Databricks Lakehouse pipelines."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "maven_market_uc",
        schema: str = "audit",
        table: str = "audit_logs",
        environment: str = "dev"
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.environment = environment
        self.full_table_path = f"{catalog}.{schema}.{table}"
        self._table_verified = False

    def ensure_audit_table(self) -> None:
        """Create the audit schema and table once per execution lifecycle."""
        if self._table_verified:
            return

        quoted_catalog = f"`{self.catalog.replace('`', '``')}`"
        quoted_schema = f"`{self.schema.replace('`', '``')}`"
        quoted_table = f"`{self.table.replace('`', '``')}`"

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_catalog}.{quoted_schema}")
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {quoted_catalog}.{quoted_schema}.{quoted_table} (
                timestamp TIMESTAMP NOT NULL,
                environment STRING NOT NULL,
                pipeline_name STRING NOT NULL,
                step_name STRING NOT NULL,
                status STRING NOT NULL,
                records_processed BIGINT NOT NULL,
                execution_time_seconds DOUBLE,
                error_message STRING,
                metadata STRING
            )
            USING DELTA
            """
        )
        self._table_verified = True

    def _get_utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _get_current_user(self) -> str:
        try:
            return self.spark.sql("SELECT current_user()").first()[0]
        except Exception:
            return "system"

    def log_event(
        self,
        pipeline_name: str,
        step_name: str,
        status: str,
        records_processed: int = 0,
        execution_time_seconds: Optional[float] = None,
        error_message: Optional[str] = None,
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Appends a structured log entry to the audit Delta table."""
        now = self._get_utc_now()
        payload = additional_metadata or {}
        if "user" not in payload:
            payload["user"] = self._get_current_user()

        log_entry = [(
            now,
            self.environment,
            pipeline_name,
            step_name,
            status.upper(),
            int(records_processed),
            float(execution_time_seconds) if execution_time_seconds is not None else 0.0,
            error_message if error_message else "",
            json.dumps(payload, default=str, sort_keys=True)
        )]

        schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("environment", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("step_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", LongType(), False),
            StructField("execution_time_seconds", DoubleType(), True),
            StructField("error_message", StringType(), True),
            StructField("metadata", StringType(), True)
        ])

        try:
            self.ensure_audit_table()
            df_log = self.spark.createDataFrame(log_entry, schema=schema)
            df_log.write.format("delta").mode("append").saveAsTable(self.full_table_path)
        except Exception as err:
            print(f"[TELEMETRY ERROR] Could not write to {self.full_table_path}: {err}", file=sys.stderr)

    def log_exception(
        self,
        pipeline_name: str,
        step_name: str,
        exception: Exception,
        execution_time_seconds: Optional[float] = None,
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Captures execution failures and formats un-truncated tracebacks."""
        formatted_traceback = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        error_desc = f"{str(exception)} | Full Trace: {formatted_traceback[:3000]}"
        
        self.log_event(
            pipeline_name=pipeline_name,
            step_name=step_name,
            status="FAILED",
            records_processed=0,
            execution_time_seconds=execution_time_seconds,
            error_message=error_desc,
            additional_metadata=additional_metadata
        )