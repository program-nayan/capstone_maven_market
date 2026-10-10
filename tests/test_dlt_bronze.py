from unittest.mock import MagicMock

import pandas as pd

from tests.dlt_test_utils import load_dlt_notebook


def test_bronze_registers_csv_mongodb_and_kafka_tables(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "01_bronze.py")

    assert set(loaded.dlt.tables) == {
        "bronze.bronze_transactions",
        "bronze.bronze_stores",
        "bronze_mongodb_customers",
        "bronze_mongodb_products",
        "bronze.bronze_kafka_orders",
        "bronze.bronze_kafka_inventory"
    }
    assert all(
        table["metadata"]["table_properties"] == {"quality": "bronze"}
        for table in loaded.dlt.tables.values()
    )


def test_bronze_csv_table_reads_autoloader_and_adds_source_metadata(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "01_bronze.py")
    dataframe = MagicMock()
    reader = loaded.spark.readStream.format.return_value
    reader.option.return_value.option.return_value.option.return_value.load.return_value = dataframe

    result = loaded.dlt.tables["bronze.bronze_transactions"]["function"]()

    loaded.spark.readStream.format.assert_called_once_with("cloudFiles")
    assert "option('cloudFiles.format', 'csv')" in str(reader.mock_calls)
    assert "option('header', 'true')" in str(reader.mock_calls)
    assert "option('inferSchema', 'true')" in str(reader.mock_calls)
    reader.option.return_value.option.return_value.option.return_value.load.assert_called_once_with(
        "abfss://container@storage/raw/transactions/"
    )
    assert dataframe.withColumn.call_args.args[0] == "_ingested_at"
    assert dataframe.withColumn.return_value.withColumn.call_args.args[0] == "_source_file"
    assert (
        dataframe.withColumn.return_value.withColumn.return_value.withColumn
        .call_args.args[0]
        == "_source_file_name"
    )
    assert result is dataframe.withColumn.return_value.withColumn.return_value.withColumn.return_value


def test_bronze_mongodb_converts_documents_and_adds_source_system(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "01_bronze.py")
    documents = [{"customer_id": 17, "customer_name": "A"}]
    collection = MagicMock()
    collection.find.return_value = documents
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = collection

    import pymongo
    monkeypatch.setattr(pymongo, "MongoClient", lambda uri: client)
    dataframe = MagicMock()
    loaded.spark.createDataFrame.return_value = dataframe

    result = loaded.dlt.tables["bronze_mongodb_customers"]["function"]()

    input_frame = loaded.spark.createDataFrame.call_args.args[0]
    assert isinstance(input_frame, pd.DataFrame)
    assert input_frame.to_dict("records") == documents
    assert result is dataframe.withColumn.return_value.withColumn.return_value
    source_system_call = dataframe.withColumn.return_value.withColumn.call_args
    assert source_system_call.args[0] == "_source_system"
    assert source_system_call.args[1].name == "lit(MongoDB_Atlas.customers)"


def test_bronze_kafka_credentials_are_sanitized_and_stream_topic_configured(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "01_bronze.py")
    secret_values = {
        "kafka-bootstrap": " https://broker.example:9092/ ",
        "kafka-key": " api-key ",
        "kafka-secret": " api-secret "
    }
    loaded.dbutils.secrets.get.side_effect = lambda scope, key: secret_values[key]

    bootstrap, jaas_config = loaded.globals["get_sanitized_kafka_creds"]()
    assert bootstrap == "broker.example:9092"
    assert 'username="api-key"' in jaas_config

    loaded.dlt.tables["bronze.bronze_kafka_orders"]["function"]()
    reader = loaded.spark.readStream.format.return_value
    assert "option('kafka.bootstrap.servers', 'broker.example:9092')" in str(reader.mock_calls)
    assert "option('subscribe', 'orders-topic')" in str(reader.mock_calls)
    assert "option('startingOffsets', 'earliest')" in str(reader.mock_calls)
