import pytest
from unittest.mock import MagicMock, patch
from src.utils.config_loader import ConfigLoader
from src.utils.dlt_loader import load_config


def test_config_loader_master_yaml_catalog_and_schemas():
    """Verify loading catalog name, schemas, and environment property."""
    loader = ConfigLoader(env="dev")
    assert loader.environment == "dev"
    assert loader.get_catalog() == "maven_market_uc"
    assert loader.get_schema("bronze") == "bronze"
    assert loader.get_schema("silver") == "silver"
    assert loader.get_schema("gold") == "gold"
    assert loader.get_schema("audit") == "audit"


def test_config_loader_master_yaml_full_table_names():
    """Verify 3-tier catalog table identifier generation."""
    loader = ConfigLoader(env="dev")
    assert loader.get_full_table_name("audit", "audit_logs") == "maven_market_uc.audit.audit_logs"
    assert loader.get_full_table_name("gold", "gold_fact_sales") == "maven_market_uc.gold.fact_sales"


def test_config_loader_teammate_ingestion_helpers():
    """Verify Auto Loader, Checkpoint, and MongoDB collection helpers."""
    loader = ConfigLoader(env="dev")
    assert "transactions" in loader.get_raw_csv_path("transactions_path")
    assert "autoloader_stores" in loader.get_checkpoint_path("autoloader_stores")
    assert isinstance(loader.get_mongodb_collections(), dict)


def test_config_loader_missing_file_raises_error():
    """Verify FileNotFoundError when providing an invalid config file path."""
    with pytest.raises(FileNotFoundError):
        ConfigLoader(config_path="/invalid/path/non_existent_config.yml")


def test_config_loader_invalid_schema_key():
    """Verify KeyError when requesting an unconfigured schema layer."""
    loader = ConfigLoader(env="dev")
    with pytest.raises(KeyError):
        loader.get_schema("invalid_layer_name")


def test_dlt_loader_uses_bundle_environment_and_catalog():
    mock_spark = MagicMock()
    values = {
        "bundle.environment": "prod",
        "catalog_name": "prod_catalog"
    }
    mock_spark.conf.get.side_effect = lambda key, default=None: values.get(key, default)

    with patch("src.utils.dlt_loader.SparkSession") as mock_session:
        mock_session.builder.getOrCreate.return_value = mock_spark
        config = load_config(config_path="config/config.yml")

    assert config["environment"] == "prod"
    assert config["catalog"] == "prod_catalog"