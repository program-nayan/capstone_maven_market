import sys
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)

class TelemetryLogger:
    """
    Centralized structured logging module for Databricks Lakehouse pipelines.
    Writes telemetry directly to Unity Catalog audit Delta tables with fallback logging.
    """

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

    def _get_utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

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
        """Constructs an audit log entry and appends it to the audit Delta table."""
        now = self._get_utc_now()
        
        log_entry = [(
            now,
            self.environment,
            pipeline_name,
            step_name,
            status.upper(),
            int(records_processed),
            float(execution_time_seconds) if execution_time_seconds is not None else 0.0,
            error_message if error_message else "",
            str(additional_metadata) if additional_metadata else "{}"
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
            df_log = self.spark.createDataFrame(log_entry, schema=schema)
            df_log.write.format("delta").mode("append").saveAsTable(self.full_table_path)
            
            print(f"[{now.isoformat()}] [{status.upper()}] {pipeline_name}.{step_name} | "
                  f"Records: {records_processed:,} | Duration: {execution_time_seconds or 0:.2f}s")
            
        except Exception as e:
            # Fallback printer if Delta table is locked or unreachable
            print(f"CRITICAL: Failed writing log to {self.full_table_path}: {str(e)}", file=sys.stderr)
            print(f"FALLBACK LOG: [{now.isoformat()}] [{status}] {pipeline_name}.{step_name} - Error: {error_message}", file=sys.stderr)

    def log_exception(
        self,
        pipeline_name: str,
        step_name: str,
        exception: Exception,
        execution_time_seconds: Optional[float] = None,
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Captures execution failures and formats complete tracebacks."""
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