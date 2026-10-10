from tests.dlt_test_utils import load_dlt_notebook


def test_silver_registers_expected_tables_and_quality_expectations(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "02_silver.py")

    assert set(loaded.dlt.tables) == {
        "silver.silver_stores",
        "silver.silver_products",
        "silver.silver_orders",
        "silver.silver_inventory"
    }
    assert loaded.dlt.expectations == [
        ("valid_store_id", "store_id IS NOT NULL"),
        ("valid_product_id", "product_id IS NOT NULL"),
        ("valid_price", "product_retail_price > 0"),
        ("valid_quantity", "quantity > 0"),
        ("valid_customer_id", "customer_id IS NOT NULL"),
        ("valid_stock_count", "stock_on_hand IS NOT NULL AND stock_on_hand >= 0")
    ]


def test_silver_stores_join_regions_and_remove_ingestion_columns(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "02_silver.py")
    stores = loaded.dlt.read.return_value
    regions = loaded.dlt.read.return_value.drop.return_value

    result = loaded.dlt.tables["silver.silver_stores"]["function"]()

    assert loaded.dlt.read.call_args_list[0].args == ("bronze.bronze_stores",)
    assert loaded.dlt.read.call_args_list[1].args == ("bronze.bronze_regions",)
    loaded.dlt.read.return_value.drop.assert_called_once_with(
        "_ingested_at",
        "_source_file",
        "_source_file_name",
        "_rescued_data"
    )
    stores.join.assert_called_once_with(regions, on="region_id", how="left")
    assert result is stores.join.return_value


def test_silver_products_reads_bronze_product_table(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "02_silver.py")

    result = loaded.dlt.tables["silver.silver_products"]["function"]()

    loaded.dlt.read.assert_called_once_with("bronze.bronze_mongodb_products")
    assert result is loaded.dlt.read.return_value


def test_silver_orders_parse_kafka_payload_and_join_products(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "02_silver.py")
    kafka_stream = loaded.dlt.read_stream.return_value
    products = loaded.dlt.read.return_value

    result = loaded.dlt.tables["silver.silver_orders"]["function"]()

    loaded.dlt.read_stream.assert_called_once_with("bronze.bronze_kafka_orders")
    loaded.dlt.read.assert_called_once_with("silver.silver_products")
    kafka_stream.select.assert_called_once()
    parsed_orders = kafka_stream.select.return_value.select.return_value
    parsed_orders.join.assert_called_once_with(products, on="product_id", how="inner")
    parsed_orders.join.return_value.select.assert_called_once()
    assert result is parsed_orders.join.return_value.select.return_value


def test_silver_inventory_parses_and_renames_payload_fields(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "02_silver.py")
    kafka_stream = loaded.dlt.read_stream.return_value

    result = loaded.dlt.tables["silver.silver_inventory"]["function"]()

    loaded.dlt.read_stream.assert_called_once_with("bronze.bronze_kafka_inventory")
    first_projection = kafka_stream.select.return_value
    first_projection.select.assert_called_once()
    selections = first_projection.select.call_args.args
    assert [selection.name for selection in selections] == [
        "col(data.event_id)",
        "col(data.store_id)",
        "col(data.product_id)",
        "col(data.stock_level).alias(stock_on_hand)",
        "col(data.reorder_threshold).alias(reorder_level)",
        "col(data.restock_flag)",
        "col(data.event_timestamp).alias(last_updated)",
        "col(_ingested_at)"
    ]
    assert result is first_projection.select.return_value


def test_silver_configures_scd_type_two_tables(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "02_silver.py")

    assert [call.kwargs["name"] for call in loaded.dlt.create_streaming_table.call_args_list] == [
        "silver.silver_dim_customers_scd",
        "silver.silver_dim_products_scd",
        "silver.silver_dim_stores_scd"
    ]
    assert [call.kwargs["target"] for call in loaded.dlt.apply_changes.call_args_list] == [
        "silver.silver_dim_customers_scd",
        "silver.silver_dim_products_scd",
        "silver.silver_dim_stores_scd"
    ]
    assert all(
        call.kwargs["stored_as_scd_type"] == 2
        and call.kwargs["sequence_by"] == "_ingested_at"
        for call in loaded.dlt.apply_changes.call_args_list
    )
