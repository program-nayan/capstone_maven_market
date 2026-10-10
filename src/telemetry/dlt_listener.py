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
    Reads execution metrics from DLT system event logs using the event_log() 
    table function and converts them into audit entries in audit.audit_logs.
    """
    logger = TelemetryLogger(spark=spark, catalog=catalog, environment=environment)
    
    try:
        # Databricks DLT system event log function
        events_df = spark.sql(f"""
            SELECT 
                timestamp,
                event_type,
                message,
                details
            FROM event_log('{pipeline_id}')
            WHERE event_type IN ('flow_progress', 'pipeline_state_change')
            ORDER BY timestamp DESC
            LIMIT 50
        """)
        
        rows = events_df.collect()
        if not rows:
            print(f"No system events found for pipeline ID: {pipeline_id}")
            return

        for row in rows:
            details_json = json.loads(row["details"]) if row["details"] else {}
            flow_name = details_json.get("flow_name", "dlt_pipeline_step")
            
            # Extract output row counts from event metadata
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
            pipeline_name=f"dlt_pipeline_{pipeline_id[:8]}",
            step_name="capture_events",
            exception=e
        )
        raise e