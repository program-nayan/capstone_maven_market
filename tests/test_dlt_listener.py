import pytest
from unittest.mock import MagicMock, patch
from src.telemetry.dlt_listener import capture_dlt_pipeline_events


def test_capture_dlt_pipeline_events_mock():
    mock_spark = MagicMock()
    
    # Mock return rows from event log query
    mock_row = MagicMock()
    mock_row.__getitem__.side_effect = lambda key: {
        "timestamp": "2026-10-09 10:00:00",
        "pipeline_id": "dlt-12345",
        "event_type": "flow_progress",
        "message": "Flow completed",
        "details": '{"flow_name": "clean_orders", "flow_progress": {"metrics": {"num_output_rows": 500}}}'
    }[key]
    
    mock_spark.sql.return_value.collect.return_value = [mock_row]
    
    with patch("src.telemetry.dlt_listener.TelemetryLogger") as MockLogger:
        mock_logger_inst = MockLogger.return_value
        capture_dlt_pipeline_events(
            spark=mock_spark,
            pipeline_id="dlt-12345",
            catalog="maven_market_uc",
            environment="dev"
        )
        
        assert mock_logger_inst.log_event.called