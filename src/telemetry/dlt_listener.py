import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Sequence
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)
from src.telemetry.logger import TelemetryLogger


def _parse_details(value: object) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("DLT event details must decode to a JSON object.")
        return parsed
    raise TypeError(f"Unsupported DLT event details type: {type(value).__name__}")


def _extract_flow_name(details: dict) -> Optional[str]:
    """Safely extracts flow or table names from DLT event details without throwing AttributeError."""
    if not isinstance(details, dict):
        return None
        
    if details.get("flow_name"):
        return str(details["flow_name"])
        
    for key in ("flow_progress", "pipeline_progress", "update_progress"):
        block = details.get(key)
        if isinstance(block, dict):
            if block.get("name"):
                return str(block["name"])
            status = block.get("status")
            if isinstance(status, dict) and status.get("name"):
                return str(status["name"])
    return None


def _event_status(event_type: str, details: dict) -> str:
    if not isinstance(details, dict):
        return "IN_PROGRESS"
    state = details.get("state", "")
    for event_key in (
        "flow_progress",
        "pipeline_state_change",
        "pipeline_progress",
        "update_progress"
    ):
        event_details = details.get(event_key, {})
        if isinstance(event_details, dict):
            status_or_state = event_details.get("status") or event_details.get("state")
            if isinstance(status_or_state, (str, int)):
                state = status_or_state
            elif isinstance(status_or_state, dict):
                state = status_or_state.get("state") or status_or_state.get("status") or state

    normalized_state = str(state).upper()
    if "FAIL" in normalized_state or "ERROR" in normalized_state:
        return "FAILED"
    if normalized_state in {"COMPLETED", "SUCCESS", "SUCCEEDED"}:
        return "SUCCESS"
    return "IN_PROGRESS"


def capture_dlt_pipeline_events(
    spark: SparkSession,
    pipeline_id: str,
    catalog: str = "maven_market_uc",
    environment: str = "dev"
) -> None:
    """
    Reads execution metrics from DLT system event logs using event_log()
    and batch-appends structured entries into audit.audit_logs.
    """
    logger = TelemetryLogger(spark=spark, catalog=catalog, environment=environment)
    logger.ensure_audit_table()
    pipeline_name = f"dlt_pipeline_{pipeline_id[:8]}"
    
    try:
        escaped_pipeline_name = pipeline_name.replace("'", "''")
        
        # 1. Fetch timestamp watermark from existing audit logs
        watermark_row = spark.sql(
            f"""
            SELECT MAX(
                try_cast(get_json_object(metadata, '$.event_timestamp') AS TIMESTAMP)
            ) AS last_event_timestamp
            FROM {logger.full_table_path}
            WHERE pipeline_name = '{escaped_pipeline_name}'
            """
        ).first()
        
        last_event_timestamp = watermark_row["last_event_timestamp"] if watermark_row else None
        timestamp_filter = ""
        if last_event_timestamp is not None:
            watermark = (
                last_event_timestamp.isoformat(sep=" ")
                if isinstance(last_event_timestamp, datetime)
                else str(last_event_timestamp)
            )
            timestamp_filter = f"AND timestamp >= TIMESTAMP '{watermark}'"

        # 2. Fetch existing event keys to avoid duplicate writes
        existing_event_keys = {
            row["event_key"]
            for row in spark.sql(
                f"""
                SELECT DISTINCT get_json_object(metadata, '$.event_key') AS event_key
                FROM {logger.full_table_path}
                WHERE pipeline_name = '{escaped_pipeline_name}'
                  AND get_json_object(metadata, '$.event_key') IS NOT NULL
                """
            ).collect()
        }

        escaped_pipeline_id = pipeline_id.replace("'", "''")
        
        # 3. Query DLT system event log function
        events_df = spark.sql(f"""
            SELECT 
                timestamp,
                event_type,
                message,
                details
            FROM event_log('{escaped_pipeline_id}')
            WHERE event_type IN (
                'flow_progress',
                'pipeline_state_change',
                'pipeline_progress',
                'update_progress'
            )
                {timestamp_filter}
            ORDER BY timestamp ASC
        """)
        
        rows = events_df.collect()
        if not rows:
            logger.log_event(
                pipeline_name=pipeline_name,
                step_name="capture_events",
                status="NO_EVENTS",
                additional_metadata={"pipeline_id": pipeline_id}
            )
            return

        batch_log_entries = []
        now_utc = datetime.now(timezone.utc)

        # 4. Collect and transform events safely in memory
        for row in rows:
            details_json = _parse_details(row["details"])
            
            # Resolve actual flow/table name
            raw_flow = _extract_flow_name(details_json)
            if raw_flow:
                flow_name = raw_flow.split(".")[-1]
            else:
                flow_name = "pipeline_update" if row["event_type"] == "update_progress" else "dlt_flow"
            
            # Extract row metrics safely
            flow_progress = details_json.get("flow_progress", {}) if isinstance(details_json, dict) else {}
            metrics = flow_progress.get("metrics", {}) if isinstance(flow_progress, dict) else {}
            records_processed = int(metrics.get("num_output_rows", 0) or 0) if isinstance(metrics, dict) else 0
            
            event_timestamp = row["timestamp"]
            timestamp_value = (
                event_timestamp.isoformat()
                if hasattr(event_timestamp, "isoformat")
                else str(event_timestamp)
            )
            event_type = row["event_type"]
            message = row["message"]
            
            # Generate SHA256 event hash
            event_key = hashlib.sha256(
                "|".join(
                    (
                        pipeline_id,
                        timestamp_value,
                        event_type,
                        flow_name,
                        str(message or ""),
                        json.dumps(details_json, default=str, sort_keys=True)
                    )
                ).encode("utf-8")
            ).hexdigest()
            
            if event_key in existing_event_keys:
                continue

            metadata_payload = json.dumps({
                "event_key": event_key,
                "event_timestamp": timestamp_value,
                "event_type": event_type,
                "pipeline_id": pipeline_id,
                "raw_message": message
            })

            batch_log_entries.append((
                now_utc,
                environment,
                pipeline_name,
                flow_name,
                _event_status(event_type, details_json),
                records_processed,
                None,
                "",
                metadata_payload
            ))
            existing_event_keys.add(event_key)

        if not batch_log_entries:
            print(f"No new distinct events to log for pipeline {pipeline_id}")
            return

        # 5. Define schema & execute a SINGLE batch write transaction
        audit_schema = StructType([
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

        batch_df = spark.createDataFrame(batch_log_entries, schema=audit_schema)
        batch_df.write.format("delta").mode("append").saveAsTable(logger.full_table_path)
        print(f"Successfully batch-logged {len(batch_log_entries)} DLT events to {logger.full_table_path}")

    except Exception as e:
        logger.log_exception(
            pipeline_name=pipeline_name,
            step_name="capture_events",
            exception=e
        )
        raise