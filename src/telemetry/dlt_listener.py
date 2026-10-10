import hashlib
import json
from datetime import datetime
from pyspark.sql import SparkSession
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


def _event_status(event_type: str, details: dict) -> str:
    state = details.get("state", "")
    for event_key in (
        "flow_progress",
        "pipeline_state_change",
        "pipeline_progress",
        "update_progress"
    ):
        event_details = details.get(event_key, {})
        if isinstance(event_details, dict):
            state = (
                event_details.get("status")
                or event_details.get("state")
                or state
            )
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
    Reads execution metrics from DLT system event logs using the event_log() 
    table function and converts them into audit entries in audit.audit_logs.
    """
    logger = TelemetryLogger(spark=spark, catalog=catalog, environment=environment)
    logger.ensure_audit_table()
    pipeline_name = f"dlt_pipeline_{pipeline_id[:8]}"
    
    try:
        escaped_pipeline_name = pipeline_name.replace("'", "''")
        watermark_row = spark.sql(
            f"""
            SELECT MAX(
                try_cast(get_json_object(metadata, '$.event_timestamp') AS TIMESTAMP)
            ) AS last_event_timestamp
            FROM {logger.full_table_path}
            WHERE pipeline_name = '{escaped_pipeline_name}'
            """
        ).first()
        last_event_timestamp = (
            watermark_row["last_event_timestamp"] if watermark_row else None
        )
        timestamp_filter = ""
        if last_event_timestamp is not None:
            if isinstance(last_event_timestamp, datetime):
                watermark = last_event_timestamp.isoformat(sep=" ")
            else:
                watermark = str(last_event_timestamp)
            timestamp_filter = f"AND timestamp >= TIMESTAMP '{watermark}'"

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
        # Databricks DLT system event log function
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

        for row in rows:
            details_json = _parse_details(row["details"])
            flow_name = details_json.get("flow_name", "dlt_pipeline_step")
            
            # Extract output row counts from event metadata
            metrics = details_json.get("flow_progress", {}).get("metrics", {})
            records_processed = int(metrics.get("num_output_rows", 0) or 0)
            event_timestamp = row["timestamp"]
            timestamp_value = (
                event_timestamp.isoformat()
                if hasattr(event_timestamp, "isoformat")
                else str(event_timestamp)
            )
            event_type = row["event_type"]
            message = row["message"]
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

            logger.log_event(
                pipeline_name=pipeline_name,
                step_name=flow_name,
                status=_event_status(event_type, details_json),
                records_processed=records_processed,
                additional_metadata={
                    "event_key": event_key,
                    "event_timestamp": timestamp_value,
                    "event_type": event_type,
                    "pipeline_id": pipeline_id,
                    "raw_message": message
                }
            )
            existing_event_keys.add(event_key)
            
    except Exception as e:
        logger.log_exception(
            pipeline_name=pipeline_name,
            step_name="capture_events",
            exception=e
        )
        raise