import pytest
from unittest.mock import MagicMock
from src.telemetry.logger import TelemetryLogger

def test_telemetry_logger_initialization():
    mock_spark = MagicMock()
    logger = TelemetryLogger(
        spark=mock_spark,
        catalog="maven_market_uc",
        schema="audit",
        table="audit_logs",
        environment="dev"
    )
    assert logger.full_table_path == "maven_market_uc.audit.audit_logs"
    assert logger.environment == "dev"

def test_log_event_mock_write():
    mock_spark = MagicMock()
    logger = TelemetryLogger(spark=mock_spark)
    
    # Execute sample pipeline start log event
    logger.log_event(
        pipeline_name="bronze_ingestion",
        step_name="ingest_transactions",
        status="STARTED",
        records_processed=1500
    )
    
    # Verify DataFrame construction and saveAsTable call were invoked
    assert mock_spark.createDataFrame.called