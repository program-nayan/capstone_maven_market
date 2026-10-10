import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.telemetry.logger import TelemetryLogger

def capture_dlt_pipeline_telemetry(
    spark: SparkSession, 
    dlt_table_name: str = "maven_market_uc.silver.silver_orders",
    pipeline_name: str = "dlt_medallion_pipeline"
):
    """
    Reads execution metrics and data quality expectation statistics directly 
    from DLT's built-in event log and writes them to audit.audit_logs.
    """
    logger = TelemetryLogger(spark=spark, catalog="maven_market_uc", schema="audit", table="audit_logs")
    
    try:
        # Use DLT's native event_log table function instead of a non-existent table path
        events_df = spark.sql(f"SELECT * FROM event_log(TABLE({dlt_table_name}))")
        
        # Filter for flow progress events containing record counts
        flow_events = events_df.filter("event_type = 'flow_progress'").orderBy(col("timestamp").desc()).limit(10)
        
        for row in flow_events.collect():
            details = json.loads(row["details"]) if row["details"] else {}
            flow_name = details.get("flow_progress", {}).get("status", {}).get("name", "dlt_flow")
            metrics = details.get("flow_progress", {}).get("metrics", {})
            
            num_records = metrics.get("num_output_rows", 0)
            
            logger.log_event(
                pipeline_name=pipeline_name,
                step_name=flow_name,
                status="SUCCESS",
                records_processed=int(num_records),
                additional_metadata={"dlt_event_id": row["id"]}
            )
            
        print(" Successfully synced DLT event metrics into audit.audit_logs!")

    except Exception as e:
        logger.log_exception(
            pipeline_name=pipeline_name,
            step_name="dlt_event_log_sync",
            exception=e
        )

# Execute telemetry sync after DLT run
if __name__ == "__main__":
    capture_dlt_pipeline_telemetry(spark)