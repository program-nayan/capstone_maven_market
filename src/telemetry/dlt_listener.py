import hashlib
import json
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)
from src.telemetry.logger import TelemetryLogger


def capture_dlt_pipeline_events(
    spark: SparkSession,
    pipeline_id: str,
    catalog: str = "maven_market_uc",
    environment: str = "dev"
) -> None:
    """
    Extracts aggregated flow telemetry (durations, records, expectations)
    directly from DLT event_log and appends clean records to audit.audit_logs.
    """
    logger = TelemetryLogger(spark=spark, catalog=catalog, environment=environment)
    logger.ensure_audit_table()
    pipeline_name = f"dlt_pipeline_{pipeline_id[:8]}"

    try:
        escaped_pipeline_id = pipeline_id.replace("'", "''")
        escaped_pipeline_name = pipeline_name.replace("'", "''")

        # 1. Fetch timestamp watermark from audit logs to prevent duplicate ingests
        watermark_row = spark.sql(
            f"""
            SELECT MAX(
                try_cast(get_json_object(metadata, '$.event_timestamp') AS TIMESTAMP)
            ) AS last_event_timestamp
            FROM {logger.full_table_path}
            WHERE pipeline_name = '{escaped_pipeline_name}'
            """
        ).first()

        last_timestamp = watermark_row["last_event_timestamp"] if watermark_row else None
        timestamp_filter = ""
        if last_timestamp:
            watermark = (
                last_timestamp.isoformat(sep=" ")
                if isinstance(last_timestamp, datetime)
                else str(last_timestamp)
            )
            timestamp_filter = f"AND timestamp >= TIMESTAMP '{watermark}'"

        # 2. Fetch existing event keys (deduplication over the last 7 days)
        existing_keys = {
            row["event_key"]
            for row in spark.sql(
                f"""
                SELECT DISTINCT get_json_object(metadata, '$.event_key') AS event_key
                FROM {logger.full_table_path}
                WHERE pipeline_name = '{escaped_pipeline_name}'
                  AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
                  AND get_json_object(metadata, '$.event_key') IS NOT NULL
                """
            ).collect()
        }

        # 3. Aggregate flow lifecycle events into 1 row per table per pipeline update
        agg_sql = f"""
        WITH raw_flow_events AS (
            SELECT 
                origin.update_id AS update_id,
                origin.flow_name AS raw_flow_name,
                timestamp,
                details:flow_progress.status::STRING AS flow_status,
                TRY_CAST(details:flow_progress.metrics.num_output_rows AS BIGINT) AS output_rows,
                details:flow_progress.data_quality.expectations::STRING AS expectations_raw,
                COALESCE(details:error.message::STRING, message, '') AS raw_message
            FROM event_log('{escaped_pipeline_id}')
            WHERE event_type = 'flow_progress'
              AND origin.flow_name IS NOT NULL
              AND origin.flow_name != 'pipelines.flowTimeMetrics.missingFlowName'
              {timestamp_filter}
        ),
        flow_summaries AS (
            SELECT 
                update_id,
                element_at(split(raw_flow_name, '[.]'), -1) AS step_name,
                MIN(timestamp) AS start_ts,
                MAX(timestamp) AS end_ts,
                CASE 
                    WHEN COUNT_IF(flow_status = 'FAILED' OR raw_message LIKE '%FAILED%' OR raw_message LIKE '%Failed%') > 0 
                    THEN 'FAILED' 
                    ELSE 'SUCCESS' 
                END AS status,
                COALESCE(MAX(output_rows), 0) AS records_processed,
                MAX_BY(
                    expectations_raw, 
                    CASE WHEN expectations_raw IS NOT NULL AND expectations_raw NOT IN ('', 'null', '[]') THEN timestamp ELSE NULL END
                ) AS expectations_json,
                MAX_BY(
                    CASE WHEN raw_message != '' THEN raw_message ELSE NULL END, 
                    timestamp
                ) AS error_msg
            FROM raw_flow_events
            GROUP BY update_id, raw_flow_name
        )
        SELECT 
            end_ts AS timestamp,
            step_name,
            status,
            records_processed,
            ROUND(CAST(unix_millis(end_ts) - unix_millis(start_ts) AS DOUBLE) / 1000.0, 2) AS execution_time_seconds,
            COALESCE(error_msg, '') AS error_msg,
            update_id,
            expectations_json
        FROM flow_summaries
        ORDER BY end_ts ASC
        """

        aggregated_df = spark.sql(agg_sql)
        rows = aggregated_df.collect()

        if not rows:
            print(f"No new flow events found for pipeline {pipeline_id}")
            return

        batch_log_entries = []

        for row in rows:
            step_name = row["step_name"]
            update_id = row["update_id"]
            end_ts = row["timestamp"]
            end_ts_str = end_ts.isoformat() if hasattr(end_ts, "isoformat") else str(end_ts)
            
            # Unique deterministic key per table flow per update execution
            event_key = hashlib.sha256(
                f"{pipeline_id}|{update_id}|{step_name}".encode("utf-8")
            ).hexdigest()

            if event_key in existing_keys:
                continue

            # Safely parse expectations JSON string into Python list
            exp_raw = row["expectations_json"]
            parsed_expectations = []
            if exp_raw and exp_raw not in ("null", "None", ""):
                try:
                    loaded = json.loads(exp_raw)
                    if isinstance(loaded, list):
                        parsed_expectations = loaded
                    elif isinstance(loaded, dict):
                        parsed_expectations = [loaded]
                except Exception:
                    parsed_expectations = []

            metadata_payload = json.dumps({
                "event_key": event_key,
                "event_timestamp": end_ts_str,
                "update_id": update_id,
                "pipeline_id": pipeline_id,
                "expectations_summary": parsed_expectations
            })

            batch_log_entries.append((
                end_ts if isinstance(end_ts, datetime) else datetime.fromisoformat(end_ts_str),
                environment,
                pipeline_name,
                step_name,
                row["status"],
                int(row["records_processed"]),
                float(row["execution_time_seconds"]) if row["execution_time_seconds"] is not None else 0.0,
                row["error_msg"] if row["status"] == "FAILED" else "",
                metadata_payload
            ))
            existing_keys.add(event_key)

        if not batch_log_entries:
            print("All events have already been committed to audit_logs.")
            return

        # 4. Atomic batch append into audit.audit_logs
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
        print(f"Successfully synced {len(batch_log_entries)} consolidated flow records to {logger.full_table_path}")

    except Exception as e:
        logger.log_exception(
            pipeline_name=pipeline_name,
            step_name="capture_events",
            exception=e
        )
        raise