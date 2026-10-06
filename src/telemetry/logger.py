import sys
import trace
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType


class TelemetryLogger:
    """
    Centralized structured logging module for Databricks Lakehouse pipelines.
    Formats logs into a standard schema and writes logs to Delta tables or stdout.
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

    def _get_utc_now((self) -> datetime:
        return datetime.now(timezone.utc)

    def log_event(
        self,
        pipeline_name: str,
        step_name: str,
        status: str,
        records_processed: int = 0,
        error_message: Optional[str] = None,
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Constructs a structured audit log entry and appends it to the audit Delta table.
        
        Status options: 'STARTED', 'IN_PROGRESS', 'SUCCESS', 'FAILED', 'WARNING'
        """
        now = self._get_utc_now()
        
        # Build raw audit record
        log_entry = [(
            now,
            self.environment,
            pipeline_name,
            step_name,
            status.upper(),
            int(records_processed),
            error_message if error_message else "",
            str(additional_metadata) if additional_metadata else "{}"
        )]

        # Explicit PySpark Schema definition
        schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("environment", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("step_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", LongType(), False),
            StructField("error_message", StringType(), True),
            StructField("metadata", StringType(), True)
        ])

        try:
            # Convert record to PySpark DataFrame
            df_log = self.spark.createDataFrame(log_entry, schema=schema)
            
            # Append log record to Unity Catalog Delta Audit Table
            df_log.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(self.full_table_path)
                
            print(f"[{now.isoformat()}] [{status.upper()}] {pipeline_name}.{step_name} - Records: {records_processed}")
            
        except Exception as e:
            # Fallback stdout printer if Delta table is unreachable
            print(f"CRITICAL: Failed writing log to {self.full_table_path}: {str(e)}", file=sys.stderr)
            print(f"FALLBACK LOG: [{now.isoformat()}] [{status}] {pipeline_name}.{step_name} - Error: {error_message}", file=sys.stderr)

    def log_exception(
        self,
        pipeline_name: str,
        step_name: str,
        exception: Exception,
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Helper method to format, extract traceback, and capture execution failure logs.
        """
        formatted_traceback = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        error_desc = f"{str(exception)} | Trace: {formatted_traceback[:500]}" # Truncated for table stability
        
        self.log_event(
            pipeline_name=pipeline_name,
            step_name=step_name,
            status="FAILED",
            records_processed=0,
            error_message=error_desc,
            additional_metadata=additional_metadata
        )