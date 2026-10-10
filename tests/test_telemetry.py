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
    assert mock_spark.sql.call_count == 2
    mock_spark.createDataFrame.return_value.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        "maven_market_uc.audit.audit_logs"
    )


def test_log_event_surfaces_audit_setup_failure():
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = RuntimeError("No permission to create audit schema")
    logger = TelemetryLogger(spark=mock_spark)

    with pytest.raises(RuntimeError, match="No permission to create audit schema"):
        logger.log_event(
            pipeline_name="test_pipeline",
            step_name="audit_setup",
            status="STARTED"
        )