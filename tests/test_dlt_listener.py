import json
import pytest
from unittest.mock import MagicMock, patch
from src.telemetry.dlt_listener import (
    capture_dlt_pipeline_events
)


def test_capture_dlt_pipeline_events_mock():
    mock_spark = MagicMock()

    mock_row = MagicMock()
    mock_row.__getitem__.side_effect = lambda key: {
        "step_name": "clean_orders",
        "update_id": "update-001",
        "timestamp": "2026-10-09 10:00:00",
        "status": "SUCCESS",
        "records_processed": 500,
        "execution_time_seconds": 30.5,
        "error_msg": "",
        "expectations_json": "[]"
    }[key]

    watermark_result = MagicMock()
    watermark_result.first.return_value = None
    existing_keys_result = MagicMock()
    existing_keys_result.collect.return_value = []
    events_result = MagicMock()
    events_result.collect.return_value = [mock_row]
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

        mock_logger_inst.log_event.assert_not_called()
        mock_spark.createDataFrame.assert_called_once()
        batch_entries = mock_spark.createDataFrame.call_args.args[0]
        assert len(batch_entries) == 1
        assert batch_entries[0][4] == "SUCCESS"
        assert "event_key" in json.loads(batch_entries[0][8])
