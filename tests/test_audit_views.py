import pytest
from unittest.mock import MagicMock
from src.telemetry.audit_views import AuditViewsManager


def test_audit_views_manager_deployment_calls():
    mock_spark = MagicMock()
    mock_config = MagicMock()
    mock_config.get_catalog.return_value = "maven_market_uc"
    mock_config.get_schema.return_value = "audit"

    manager = AuditViewsManager(spark=mock_spark, config_loader=mock_config)
    manager.deploy_all_views()

    # Verify spark.sql was executed twice (once for each view)
    assert mock_spark.sql.call_count == 2