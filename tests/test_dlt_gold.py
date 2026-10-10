from tests.dlt_test_utils import load_dlt_notebook


def test_gold_registers_expected_tables(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "03_gold.py")

    assert set(loaded.dlt.tables) == {
        "gold.fact_sales",
        "gold.fact_inventory",
        "gold.dim_customers",
        "gold.dim_stores",
        "gold.dim_products",
        "gold.dim_calendar"
    }
    assert all(
        table["metadata"]["table_properties"] == {"quality": "gold"}
        for table in loaded.dlt.tables.values()
    )


def test_gold_fact_sales_aggregates_by_date_and_dimensions(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "03_gold.py")
    orders = loaded.dlt.read.return_value

    result = loaded.dlt.tables["gold.fact_sales"]["function"]()

    loaded.dlt.read.assert_called_once_with("silver.silver_orders")
    orders.groupBy.assert_called_once()
    group_columns = orders.groupBy.call_args.args
    assert group_columns[:3] == ("store_id", "product_id", "customer_id")
    assert group_columns[3].name == "to_date(col(order_date)).alias(order_date)"
    orders.groupBy.return_value.agg.assert_called_once()
    assert result is orders.groupBy.return_value.agg.return_value


def test_gold_fact_inventory_adds_low_stock_flag(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "03_gold.py")
    inventory = loaded.dlt.read.return_value

    result = loaded.dlt.tables["gold.fact_inventory"]["function"]()

    loaded.dlt.read.assert_called_once_with("silver.silver_inventory")
    inventory.groupBy.assert_called_once_with("store_id", "product_id")
    aggregation = inventory.groupBy.return_value.agg.return_value
    aggregation.withColumn.assert_called_once()
    assert aggregation.withColumn.call_args.args[0] == "is_low_stock"
    assert result is aggregation.withColumn.return_value


def test_gold_dimensions_filter_to_current_scd_records(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "03_gold.py")
    cases = (
        ("dim_customers", "silver.silver_dim_customers_scd"),
        ("dim_stores", "silver.silver_dim_stores_scd"),
        ("dim_products", "silver.silver_dim_products_scd")
    )

    for table_name, source in cases:
        loaded.dlt.read.reset_mock()
        source_df = loaded.dlt.read.return_value

        result = loaded.dlt.tables[f"gold.{table_name}"]["function"]()

        loaded.dlt.read.assert_called_once_with(source)
        source_df.filter.assert_called_once()
        assert source_df.filter.call_args.args[0].name == "col(__END_AT).isNull()"
        source_df.filter.return_value.withColumn.assert_called_once()
        assert source_df.filter.return_value.withColumn.call_args.args[0] == "effective_from"
        assert source_df.filter.return_value.withColumn.call_args.args[1].name == "col(__START_AT)"
        assert result is source_df.filter.return_value.withColumn.return_value


def test_gold_calendar_dimension_reads_bronze_calendar(monkeypatch):
    loaded = load_dlt_notebook(monkeypatch, "03_gold.py")

    result = loaded.dlt.tables["gold.dim_calendar"]["function"]()

    loaded.dlt.read.assert_called_once_with("bronze.bronze_calendar")
    assert result is loaded.dlt.read.return_value
