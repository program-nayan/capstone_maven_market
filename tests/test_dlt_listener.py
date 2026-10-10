import pytest
from unittest.mock import MagicMock, patch
from src.telemetry.dlt_listener import (
    _event_status,
    capture_dlt_pipeline_events
)


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
    
    watermark_result = MagicMock()
    watermark_result.first.return_value = None
    existing_keys_result = MagicMock()
    existing_keys_result.collect.return_value = []
    events_result = MagicMock()
    events_result.collect.return_value = [mock_row, mock_row]
    mock_spark.sql.side_effect = [
        watermark_result,
        existing_keys_result,
        events_result
    ]
    
    with patch("src.telemetry.dlt_listener.TelemetryLogger") as MockLogger:
        mock_logger_inst = MockLogger.return_value
        capture_dlt_pipeline_events(
            spark=mock_spark,
            pipeline_id="dlt-12345",
            catalog="maven_market_uc",
            environment="dev"
        )
        
        assert mock_logger_inst.log_event.call_count == 1
        assert mock_logger_inst.log_event.call_args.kwargs["status"] == "IN_PROGRESS"
        assert "event_key" in mock_logger_inst.log_event.call_args.kwargs["additional_metadata"]


def test_event_status_does_not_mark_unfinished_flows_successful():
    assert _event_status("flow_progress", {}) == "IN_PROGRESS"
    assert _event_status(
        "update_progress",
        {"update_progress": {"state": "COMPLETED"}}
    ) == "SUCCESS"
    assert _event_status(
        "update_progress",
        {"update_progress": {"state": "FAILED"}}
    ) == "FAILED"