# Databricks Notebook
from src.telemetry.logger import TelemetryLogger
from src.telemetry.decorators import log_execution

# Test standard event logger directly
logger = TelemetryLogger(spark=spark, environment="dev")

logger.log_event(
    pipeline_name="day_2_validation",
    step_name="manual_test_event",
    status="SUCCESS",
    records_processed=100
)

# Test decorator wrapper
@log_execution(pipeline_name="day_2_validation", step_name="decorator_test_step")
def simulate_pipeline_step(spark):
    return spark.sql("SELECT 1 AS id, 'test_product' AS product_name")

df_result = simulate_pipeline_step(spark=spark)
display(df_result)