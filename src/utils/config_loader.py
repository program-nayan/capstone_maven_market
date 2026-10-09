import os
import yaml
from typing import Dict, Any, Optional


class ConfigLoader:
    """
    Utility class to load and parse project YAML configurations dynamically.
    Enforces non-hardcoded environment variables across pipelines and tests.
    """

    def __init__(self, config_path: Optional[str] = None, env: Optional[str] = None):
        self.env = env or os.getenv("DATABRICKS_ENV", "dev")
        
        # Resolve path relative to repository root if not specified
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

    def get_project_name(self) -> str:
        return self._config_data.get("project", {}).get("name", "maven_market")

    def get_catalog(self) -> str:
        return self._config_data.get("project", {}).get("catalog", "maven_market_uc")

    def get_schema(self, layer: str) -> str:
        """Returns target schema name for layer (bronze, silver, gold, audit)."""
        schemas = self._config_data.get("schemas", {})
        if layer not in schemas:
            raise KeyError(f"Schema layer '{layer}' not defined in config.yml")
        return schemas[layer]

    def get_full_table_name(self, layer: str, table_key: str) -> str:
        """Returns fully-qualified Unity Catalog table path: catalog.schema.table"""
        catalog = self.get_catalog()
        schema = self.get_schema(layer)
        tables = self._config_data.get("tables", {})
        
        table_name = tables.get(table_key, table_key)
        return f"{catalog}.{schema}.{table_name}"

    def get_path(self, path_key: str) -> str:
        """Resolves target storage volume or checkpoint root path."""
        paths = self._config_data.get("paths", {})
        if path_key not in paths:
            raise KeyError(f"Storage path key '{path_key}' not defined in config.yml")
        return paths[path_key]