import os
import yaml
from typing import Dict, Any, Optional


class ConfigLoader:
    """
    Utility class to load and parse the Master YAML Configuration File.
    Enforces 100% config-driven parameterization across environments.
    """

    def __init__(self, config_path: Optional[str] = None, env: Optional[str] = None):
        self.env = env or os.getenv("DATABRICKS_ENV", "dev")
        
        if config_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(base_dir, "config", "config.yml")
            
        self.config_path = config_path
        self._config_data = self._load_yaml()

    def _load_yaml(self) -> Dict[str, Any]:
        """Loads and parses the YAML configuration file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found at path: {self.config_path}")
            
        with open(self.config_path, "r", encoding="utf-8") as file:
            try:
                data = yaml.safe_load(file)
                return data if data else {}
            except yaml.YAMLError as exc:
                raise ValueError(f"Error parsing YAML configuration file: {exc}")

    def get_catalog(self) -> str:
        """Returns target Unity Catalog name."""
        return self._config_data.get("catalog", {}).get("name", "maven_market_uc")

    def get_schema(self, layer: str) -> str:
        """Returns target schema name for specified layer (bronze, silver, gold, audit)."""
        schemas = self._config_data.get("catalog", {}).get("schemas", {})
        if layer not in schemas:
            raise KeyError(f"Schema layer '{layer}' not defined in catalog.schemas config.")
        return schemas[layer]

    def get_full_table_name(self, layer: str, table_key: str) -> str:
        """
        Returns fully-qualified Unity Catalog table path: catalog.schema.table
        Handles both raw table names and schema-prefixed entries (e.g. 'audit.audit_logs').
        """
        catalog = self.get_catalog()
        schema = self.get_schema(layer)
        tables = self._config_data.get("tables", {})
        
        raw_table_name = tables.get(table_key, table_key)
        
        # Extract pure table name if schema prefix is present (e.g., 'audit.audit_logs' -> 'audit_logs')
        table_name = raw_table_name.split(".")[-1] if "." in raw_table_name else raw_table_name
            
        return f"{catalog}.{schema}.{table_name}"

    def get_storage_path(self, path_key: str) -> str:
        """Returns storage volume/mount path from 'storage' block."""
        storage = self._config_data.get("storage", {})
        if path_key not in storage:
            raise KeyError(f"Storage path key '{path_key}' not defined in config.yml")
        return storage[path_key]

    def get_source_path(self, source_key: str) -> str:
        """Returns source file glob path from 'sources' block."""
        sources = self._config_data.get("sources", {})
        if source_key not in sources:
            raise KeyError(f"Source key '{source_key}' not defined in config.yml")
        return sources[source_key]

    def get_secret_scope(self) -> str:
        """Returns secret scope name."""
        return self._config_data.get("secrets", {}).get("scope", "maven-secrets")

    def get_mongodb_config(self) -> Dict[str, str]:
        """Returns MongoDB connection parameters."""
        return self._config_data.get("mongodb", {})

    def get_kafka_config(self) -> Dict[str, Any]:
        """Returns Kafka streaming configurations and topics."""
        return self._config_data.get("kafka", {})

    def get_expectation(self, expectation_key: str) -> str:
        """Returns DLT Data Quality expectation condition string."""
        expectations = self._config_data.get("expectations", {})
        if expectation_key not in expectations:
            raise KeyError(f"Expectation key '{expectation_key}' not defined in config.yml")
        return expectations[expectation_key]