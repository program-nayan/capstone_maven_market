import pytest
from src.utils.config_loader import ConfigLoader


def test_config_loader_master_yaml_catalog_and_schemas():
    """Verify loading catalog name and schema layers from master config.yml."""
    loader = ConfigLoader(env="dev")
    assert loader.get_catalog() == "maven_market_uc"
    assert loader.get_schema("bronze") == "bronze"
    assert loader.get_schema("silver") == "silver"
    assert loader.get_schema("gold") == "gold"
    assert loader.get_schema("audit") == "audit"


def test_config_loader_master_yaml_full_table_names():
    """Verify 3-tier catalog table identifier generation from master config.yml."""
    loader = ConfigLoader(env="dev")
    assert loader.get_full_table_name("audit", "audit_logs") == "maven_market_uc.audit.audit_logs"
    assert loader.get_full_table_name("gold", "gold_fact_sales") == "maven_market_uc.gold.fact_sales"
    assert loader.get_full_table_name("silver", "silver_customers_scd") == "maven_market_uc.silver.dim_customers_scd"


def test_config_loader_master_yaml_storage_and_sources():
    """Verify storage and source file paths parsing."""
    loader = ConfigLoader(env="dev")
    assert loader.get_storage_path("base_path") == "/mnt/maven_market"
    assert loader.get_source_path("transactions_raw") == "/mnt/maven_market/bronze/raw/transactions/*.csv"


def test_config_loader_master_yaml_secrets_and_expectations():
    """Verify secret scope and DLT expectation expressions."""
    loader = ConfigLoader(env="dev")
    assert loader.get_secret_scope() == "maven-secrets"
    assert loader.get_expectation("valid_quantity") == "quantity > 0"


def test_config_loader_missing_file_raises_error():
    """Verify FileNotFoundError when providing an invalid config file path."""
    with pytest.raises(FileNotFoundError):
        ConfigLoader(config_path="/invalid/path/non_existent_config.yml")


def test_config_loader_invalid_schema_key():
    """Verify KeyError when requesting an unconfigured schema layer."""
    loader = ConfigLoader(env="dev")
    with pytest.raises(KeyError):
        loader.get_schema("invalid_layer_name")