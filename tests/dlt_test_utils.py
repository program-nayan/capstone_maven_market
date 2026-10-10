import runpy
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

from src.utils import dlt_loader


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def sample_pipeline_config():
    return {
        "environment": "test",
        "catalog": "test_catalog",
        "secret_scope": "test_scope",
        "storage": {"base_path": "abfss://container@storage"},
        "sources": {
            "pos_csv": {
                "transactions": "/raw/transactions/",
                "stores_path": "/raw/stores/"
            },
            "mongodb": {
                "database": "test_db",
                "uri_secret_key": "mongo-uri",
                "collections": {
                    "customers": "customers",
                    "products": "products"
                }
            },
            "kafka": {
                "bootstrap_secret_key": "kafka-bootstrap",
                "api_key_secret_key": "kafka-key",
                "api_secret_secret_key": "kafka-secret",
                "topics": {
                    "orders": "orders-topic",
                    "inventory": "inventory-topic"
                }
            }
        },
        "targets": {
            "schemas": {
                "bronze": "bronze",
                "silver": "silver",
                "gold": "gold"
            }
        },
        "expectations": {
            "stores": {"valid_store_id": "store_id IS NOT NULL"},
            "products": {
                "valid_product_id": "product_id IS NOT NULL",
                "valid_price": "product_retail_price > 0"
            },
            "orders": {
                "valid_quantity": "quantity > 0",
                "valid_customer_id": "customer_id IS NOT NULL"
            }
        }
    }


class FakeExpression:
    def __init__(self, name):
        self.name = name

    def alias(self, name):
        return FakeExpression(f"{self.name}.alias({name})")

    def cast(self, data_type):
        return FakeExpression(f"{self.name}.cast({data_type})")

    def isNull(self):
        return FakeExpression(f"{self.name}.isNull()")

    def __mul__(self, other):
        return FakeExpression(f"({self.name} * {other.name})")

    def __le__(self, other):
        return FakeExpression(f"({self.name} <= {other.name})")


class FakeDLT(ModuleType):
    def __init__(self):
        super().__init__("dlt")
        self.tables = {}
        self.expectations = []
        self.read = MagicMock()
        self.read_stream = MagicMock()
        self.create_streaming_table = MagicMock()
        self.apply_changes = MagicMock()

    def table(self, **metadata):
        def register(function):
            self.tables[metadata["name"]] = {
                "function": function,
                "metadata": metadata
            }
            return function
        return register

    def expect_or_drop(self, name, condition):
        self.expectations.append((name, condition))
        return lambda function: function


def load_dlt_notebook(monkeypatch, filename, config=None):
    dlt_module = FakeDLT()
    spark = MagicMock(name="spark")
    monkeypatch.setitem(sys.modules, "dlt", dlt_module)

    functions_module = ModuleType("pyspark.sql.functions")
    function_names = (
        "col",
        "from_json",
        "schema_of_json",
        "to_date",
        "sum",
        "count",
        "avg",
        "max",
        "current_timestamp",
        "input_file_name",
        "lit"
    )
    for function_name in function_names:
        setattr(
            functions_module,
            function_name,
            lambda *args, _name=function_name, **kwargs: FakeExpression(
                f"{_name}({', '.join(arg.name if isinstance(arg, FakeExpression) else str(arg) for arg in args)})"
            )
        )
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_module)
    monkeypatch.setattr(
        dlt_loader,
        "load_config",
        lambda: config or sample_pipeline_config()
    )

    runtime_module = ModuleType("databricks.sdk.runtime")
    runtime_module.dbutils = SimpleNamespace(
        secrets=SimpleNamespace(get=MagicMock(return_value=" test-secret "))
    )
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", runtime_module)

    module_globals = runpy.run_path(
        str(PROJECT_ROOT / "src" / "dlt" / filename),
        init_globals={"spark": spark}
    )
    return SimpleNamespace(
        dlt=dlt_module,
        spark=spark,
        globals=module_globals,
        dbutils=runtime_module.dbutils
    )
