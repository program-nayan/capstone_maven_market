import json
from pyspark.sql import SparkSession
from src.telemetry.logger import TelemetryLogger


def capture_dlt_pipeline_events(
    spark: SparkSession,
    pipeline_id: str,
    catalog: str = "maven_market_uc",
    environment: str = "dev"
) -> None:
    """
    Reads execution metrics and data quality expectations from DLT system event logs
    and converts them into structured audit entries inside audit.audit_logs.
    """
    logger = TelemetryLogger(spark=spark, catalog=catalog, environment=environment)
    
    # Query DLT System Event Log table
    event_log_table = f"{catalog}.{environment}.system_event_log"
    
    try:
        events_df = spark.sql(f"""
            SELECT 
                timestamp,
                pipeline_id,
                event_type,
                message,
                details
            FROM {event_log_table}
            WHERE pipeline_id = '{pipeline_id}'
              AND event_type IN ('flow_progress', 'pipeline_state_change')
            ORDER BY timestamp DESC
            LIMIT 50
        """)
        
        for row in events_df.collect():
            details_json = json.loads(row["details"]) if row["details"] else {}
            flow_name = details_json.get("flow_name", "dlt_pipeline_step")
            
            # Extract records processed metric if present
            metrics = details_json.get("flow_progress", {}).get("metrics", {})
            records_processed = metrics.get("num_output_rows", 0)
            
            logger.log_event(
                pipeline_name=f"dlt_pipeline_{pipeline_id[:8]}",
                step_name=flow_name,
                status="SUCCESS" if row["event_type"] == "flow_progress" else "IN_PROGRESS",
                records_processed=records_processed,
                additional_metadata={"raw_message": row["message"]}
            )
            
    except Exception as e:
        logger.log_exception(
            pipeline_name="dlt_telemetry_listener",
            step_name="capture_events",
            exception=e
        )