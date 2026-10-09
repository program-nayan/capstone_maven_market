import os
import yaml

class ConfigLoader:
    def __init__(self, config_path: str = "config/config.yml"):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found at path: {config_path}")
        
        with open(config_path, "r") as file:
            self._config = yaml.safe_load(file)

    @property
    def environment(self) -> str:
        return self._config.get("environment", "dev")

    @property
    def catalog(self) -> str:
        return self._config.get("catalog", "maven_market_uc")

    def get_schema(self, layer: str) -> str:
        return self._config["targets"]["schemas"].get(layer, layer)

    def get_full_table_name(self, layer: str, table_name: str) -> str:
        schema = self.get_schema(layer)
        return f"{self.catalog}.{schema}.{table_name}"

    def get_raw_csv_path(self, source_key: str) -> str:
        base_path = self._config["storage"]["base_path"].rstrip("/")
        relative_path = self._config["sources"]["pos_csv"][source_key].strip("/")
        return f"{base_path}/{relative_path}"

    def get_checkpoint_path(self, entity_name: str) -> str:
        checkpoint_base = self._config["storage"]["checkpoint_base"].rstrip("/")
        return f"{checkpoint_base}/{entity_name}"

    def get_mongodb_config(self) -> dict:
        return self._config["sources"]["mongodb"]

    def get_mongodb_collections(self) -> dict:
        """Returns map of target entity -> MongoDB collection name."""
        return self._config["sources"]["mongodb"].get("collections", {})

    def get_kafka_config(self) -> dict:
        return self._config["sources"]["kafka"]

    def get_expectation(self, rule_name: str) -> str:
        return self._config["expectations"].get(rule_name, "")