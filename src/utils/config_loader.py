import os
import yaml
from typing import Dict, Any, Optional


class ConfigLoader:
    """
    Master Configuration Loader.
    Provides complete multi-source integration for CSV Auto Loader, Kafka, MongoDB Atlas,
    and Medallion layer table generation.
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

    @property
    def environment(self) -> str:
        """Returns active deployment environment."""
        return self.env

    def get_catalog(self) -> str:
        """Returns target Unity Catalog name."""
        cat_val = self._config_data.get("catalog", "maven_market_uc")
        if isinstance(cat_val, dict):
            return cat_val.get("name", "maven_market_uc")
        elif isinstance(cat_val, str):
            return cat_val
        return "maven_market_uc"

    def get_schema(self, layer: str) -> str:
        """Returns target schema name for specified layer."""
        schemas = {}
        cat_val = self._config_data.get("catalog")
        if isinstance(cat_val, dict):
            schemas = cat_val.get("schemas", {})
        
        if not schemas:
            schemas = self._config_data.get("schemas", {})

        if layer not in schemas:
            if layer in ["bronze", "silver", "gold", "audit"]:
                return layer
            raise KeyError(f"Schema layer '{layer}' not defined in config.yml")
        return schemas[layer]

    def get_full_table_name(self, layer: str, table_key: str) -> str:
        """Returns fully-qualified Unity Catalog table path: catalog.schema.table"""
        catalog = self.get_catalog()
        schema = self.get_schema(layer)
        tables = self._config_data.get("tables", {})
        
        raw_val = tables.get(table_key)
        if not raw_val and f"{layer}_{table_key}" in tables:
            raw_val = tables[f"{layer}_{table_key}"]
        if not raw_val:
            raw_val = table_key

        raw_str = str(raw_val)
        table_name = raw_str.split(".")[-1] if "." in raw_str else raw_str

        # Alignments for Silver / Gold naming in catalog
        alias_map = {
            "silver_customers_scd": "silver_dim_customers_scd",
            "silver_products_scd": "silver_dim_products_scd",
            "silver_stores_scd": "silver_dim_stores_scd",
            "gold_fact_sales": "fact_sales",
            "gold_fact_inventory": "fact_inventory",
            "gold_dim_customers": "dim_customers",
            "gold_dim_products": "dim_products",
            "gold_dim_stores": "dim_stores",
            "gold_dim_calendar": "dim_calendar"
        }

        if table_key in alias_map:
            table_name = alias_map[table_key]

        return f"{catalog}.{schema}.{table_name}"

    def get_storage_path(self, path_key: str) -> str:
        """Returns storage path from 'storage' or 'paths' block."""
        storage = self._config_data.get("storage") or self._config_data.get("paths") or {}
        if path_key not in storage:
            if "base_path" in storage:
                return storage["base_path"]
            raise KeyError(f"Storage path key '{path_key}' not defined in config.yml")
        return storage[path_key]

    def get_raw_csv_path(self, source_key: str) -> str:
        """Returns CSV file glob path for Auto Loader ingestion."""
        sources = self._config_data.get("sources", {})
        if source_key in sources:
            return sources[source_key]
        clean_key = source_key.replace("_path", "")
        if f"{clean_key}_raw" in sources:
            return sources[f"{clean_key}_raw"]
        base_storage = self.get_storage_path("bronze_path")
        return f"{base_storage}/raw/{clean_key}/*.csv"

    def get_checkpoint_path(self, checkpoint_key: str) -> str:
        """Returns streaming checkpoint location."""
        storage = self._config_data.get("storage") or self._config_data.get("paths") or {}
        chk_base = storage.get("checkpoint_path") or storage.get("checkpoint_root") or "/mnt/maven_market/checkpoints"
        return f"{chk_base}/{checkpoint_key}"

    def get_mongodb_config(self) -> Dict[str, Any]:
        """Returns MongoDB connection parameters."""
        return self._config_data.get("mongodb", {})

    def get_mongodb_collections(self) -> Dict[str, str]:
        """Returns MongoDB entity to collection name mapping."""
        mongo_cfg = self._config_data.get("mongodb", {})
        if "collections" in mongo_cfg:
            return mongo_cfg["collections"]
        return {"products": "products", "customers": "customers"}

    def get_kafka_config(self) -> Dict[str, Any]:
        """Returns Kafka streaming configurations and topics."""
        return self._config_data.get("kafka", {})

    def get_secret_scope(self) -> str:
        """Returns secret scope name."""
        secrets = self._config_data.get("secrets", {})
        if isinstance(secrets, dict):
            return secrets.get("scope", "scope-maven-market")
        elif isinstance(secrets, str):
            return secrets
        return "scope-maven-market"

    def get_expectation(self, expectation_key: str) -> str:
        """Returns DLT Data Quality expectation condition string with defaults."""
        expectations = self._config_data.get("expectations") or self._config_data.get("dlt_expectations") or {}
        if expectation_key not in expectations:
            defaults = {
                "valid_quantity": "quantity > 0",
                "valid_price": "unit_price > 0",
                "valid_customer_id": "customer_id IS NOT NULL",
                "valid_product_id": "product_id IS NOT NULL"
            }
            if expectation_key in defaults:
                return defaults[expectation_key]
            raise KeyError(f"Expectation key '{expectation_key}' not defined in config.yml")
        return expectations[expectation_key]