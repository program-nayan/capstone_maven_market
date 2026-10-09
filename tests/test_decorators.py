import pytest
from unittest.mock import MagicMock, patch
from src.telemetry.decorators import log_execution

def test_log_execution_decorator_success():
    mock_spark = MagicMock()
    
    @log_execution(pipeline_name="test_pipeline", step_name="test_step")
    def sample_etl_step(spark):
        # Mock PySpark DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 250
        return mock_df

    with patch("src.telemetry.decorators.TelemetryLogger") as MockLogger:
        mock_logger_inst = MockLogger.return_value
        result = sample_etl_step(spark=mock_spark)
        
        # Verify logger calls
        assert mock_logger_inst.log_event.call_count == 2  # STARTED and SUCCESS
        assert result.count() == 250

def test_log_execution_decorator_failure():
    mock_spark = MagicMock()

    @log_execution(pipeline_name="test_pipeline", step_name="failing_step")
    def failing_etl_step(spark):
        raise ValueError("Simulated Ingestion Failure")

    with patch("src.telemetry.decorators.TelemetryLogger") as MockLogger:
        mock_logger_inst = MockLogger.return_value
        
        with pytest.raises(ValueError, match="Simulated Ingestion Failure"):
            failing_etl_step(spark=mock_spark)
            
        # Verify exception logging was triggered
        assert mock_logger_inst.log_exception.called