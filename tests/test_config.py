import os
import pytest
from src.utils.config_loader import ConfigLoader


def test_config_loader_default_file_exists():
    """Verify that default config.yml is present and loads standard project keys."""
    loader = ConfigLoader(env="dev")
    assert loader.get_project_name() == "maven_market"
    assert loader.get_catalog() == "maven_market_uc"
    assert loader.get_schema("audit") == "audit"


def test_config_loader_get_full_table_name():
    """Verify standard table triple identifier generation (catalog.schema.table)."""
    loader = ConfigLoader(env="dev")
    table_path = loader.get_full_table_name(layer="audit", table_key="audit_logs")
    assert table_path == "maven_market_uc.audit.audit_logs"


def test_config_loader_missing_file_raises_error():
    """Verify FileNotFoundError when providing an invalid config file path."""
    with pytest.raises(FileNotFoundError):
        ConfigLoader(config_path="/invalid/path/non_existent_config.yml")


def test_config_loader_invalid_schema_key():
    """Verify KeyError when requesting an unconfigured schema layer."""
    loader = ConfigLoader(env="dev")
    with pytest.raises(KeyError):
        loader.get_schema("non_existent_layer")


def test_config_loader_custom_yaml(tmp_path):
    """Verify configuration parsing using a temporary custom YAML file."""
    # Create temporary YAML file
    test_yaml = tmp_path / "test_config.yml"
    test_yaml.write_text(
        """
project:
  name: "unit_test_proj"
  catalog: "test_catalog"
schemas:
  bronze: "test_bronze"
tables:
  raw_events: "events_v1"
paths:
  test_path: "/Volumes/test_catalog/test_bronze/landing"
""",
        encoding="utf-8"
    )

    loader = ConfigLoader(config_path=str(test_yaml), env="test")
    assert loader.get_project_name() == "unit_test_proj"
    assert loader.get_catalog() == "test_catalog"
    assert loader.get_schema("bronze") == "test_bronze"
    assert loader.get_full_table_name("bronze", "raw_events") == "test_catalog.test_bronze.events_v1"
    assert loader.get_path("test_path") == "/Volumes/test_catalog/test_bronze/landing"