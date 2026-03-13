"""
test_metrics.py
---------------
Unit tests for the KPI computation layer (src/metrics.py).

Coverage:
  - Scalar KPI correctness (pandas engine)
  - Edge cases: empty data, all-cancelled orders, zero deliveries
  - Tabular KPI structure (top_products, monthly_revenue, top_segments)
  - SQL engine correctness via in-memory DuckDB
  - Cross-validation between pandas and SQL engines
"""

import pandas as pd
import pytest
import duckdb

from src.metrics import compute_kpis_pandas, compute_kpis_sql
from src.transform import build_fact_table, clean_deliveries, clean_orders


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def standard_tables():
    """Build a small but realistic set of clean tables for KPI testing."""
    customers = pd.DataFrame({
        "customer_id": ["CUST001", "CUST002", "CUST003"],
        "customer_name": ["Alice Martin", "Bob Durand", "Carol Simon"],
        "segment": ["Enterprise", "SMB", "Startup"],
        "city": ["Paris", "Lyon", "Marseille"],
        "country": ["France", "France", "France"],
        "signup_date": pd.to_datetime(["2022-01-15", "2021-06-30", "2020-05-01"]),
    })
    products = pd.DataFrame({
        "product_id": ["PROD001", "PROD002", "PROD003"],
        "product_name": ["CRM Pro", "Server Rack", "Consulting Pack"],
        "category": ["Software", "Hardware", "Services"],
        "unit_cost": [1200.0, 4500.0, 2000.0],
    })
    orders_raw = pd.DataFrame({
        "order_id": ["ORD001", "ORD002", "ORD003", "ORD004", "ORD005"],
        "customer_id": ["CUST001", "CUST002", "CUST001", "CUST003", "CUST002"],
        "product_id": ["PROD001", "PROD002", "PROD003", "PROD001", "PROD002"],
        "order_date": ["2023-01-10", "2023-02-14", "2023-03-05", "2023-03-20", "2023-04-15"],
        "quantity": ["2", "1", "3", "1", "2"],
        "unit_price": ["1500.00", "5500.00", "2500.00", "1500.00", "5500.00"],
        "order_status": ["completed", "completed", "cancelled", "completed", "completed"],
    })
    deliveries_raw = pd.DataFrame({
        "delivery_id": ["DEL001", "DEL002", "DEL003", "DEL004"],
        "order_id": ["ORD001", "ORD002", "ORD004", "ORD005"],
        "carrier": ["DHL", "FedEx", "DHL", "UPS"],
        "shipped_date": ["2023-01-11", "2023-02-15", "2023-03-21", "2023-04-16"],
        "delivered_date": ["2023-01-14", "2023-02-18", "2023-03-30", "2023-04-20"],
        "delivery_status": ["on_time", "on_time", "late", "on_time"],
    })
    orders = clean_orders(orders_raw)
    deliveries = clean_deliveries(deliveries_raw)
    fact = build_fact_table(orders, customers, products, deliveries)
    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "deliveries": deliveries,
        "fact": fact,
    }


@pytest.fixture
def in_memory_db(standard_tables):
    """Load standard_tables into an in-memory DuckDB for SQL KPI tests."""
    conn = duckdb.connect(":memory:")
    for name, df in standard_tables.items():
        conn.register(f"_df_{name}", df)
        conn.execute(f"CREATE TABLE {name} AS SELECT * FROM _df_{name}")
    yield conn
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Pandas KPI engine tests
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeKpisPandas:
    def test_total_orders(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        assert kpis["total_orders"] == 5

    def test_total_customers(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        assert kpis["total_customers"] == 3

    def test_total_revenue_only_completed(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        # Completed: ORD001 (2*1500=3000), ORD002 (1*5500=5500),
        #            ORD004 (1*1500=1500), ORD005 (2*5500=11000) → 21000
        assert kpis["total_revenue"] == pytest.approx(21000.0, abs=1.0)

    def test_cancellation_rate(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        # 1 cancelled / 5 total = 0.2
        assert kpis["cancellation_rate"] == pytest.approx(0.2, abs=0.001)

    def test_on_time_delivery_rate(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        # 3 on_time / 4 total delivered = 0.75
        assert kpis["on_time_delivery_rate"] == pytest.approx(0.75, abs=0.001)

    def test_average_order_value_positive(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        assert kpis["average_order_value"] > 0

    def test_top_products_structure(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        df = kpis["top_products"]
        assert isinstance(df, pd.DataFrame)
        assert "product_name" in df.columns
        assert "total_revenue" in df.columns
        # Should be sorted descending
        revenues = df["total_revenue"].tolist()
        assert revenues == sorted(revenues, reverse=True)

    def test_monthly_revenue_sorted(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        mr = kpis["monthly_revenue"]
        months = mr["year_month"].tolist()
        assert months == sorted(months)

    def test_top_segments_not_empty(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        assert len(kpis["top_segments"]) > 0

    def test_average_delivery_delay_positive(self, standard_tables):
        kpis = compute_kpis_pandas(standard_tables)
        assert kpis["average_delivery_delay_days"] > 0

    def test_all_cancelled_revenue_is_zero(self):
        """If all orders are cancelled, revenue should be 0."""
        customers = pd.DataFrame({
            "customer_id": ["C1"],
            "customer_name": ["Alice"],
            "segment": ["SMB"],
            "city": ["Paris"],
            "country": ["France"],
            "signup_date": pd.to_datetime(["2022-01-01"]),
        })
        products = pd.DataFrame({
            "product_id": ["P1"],
            "product_name": ["Tool"],
            "category": ["Software"],
            "unit_cost": [100.0],
        })
        orders = clean_orders(pd.DataFrame({
            "order_id": ["O1", "O2"],
            "customer_id": ["C1", "C1"],
            "product_id": ["P1", "P1"],
            "order_date": ["2023-01-01", "2023-02-01"],
            "quantity": ["1", "2"],
            "unit_price": ["150.0", "150.0"],
            "order_status": ["cancelled", "cancelled"],
        }))
        deliveries = clean_deliveries(pd.DataFrame({
            "delivery_id": pd.Series([], dtype=str),
            "order_id": pd.Series([], dtype=str),
            "carrier": pd.Series([], dtype=str),
            "shipped_date": pd.Series([], dtype=str),
            "delivered_date": pd.Series([], dtype=str),
            "delivery_status": pd.Series([], dtype=str),
        }))
        fact = build_fact_table(orders, customers, products, deliveries)
        tables = {
            "customers": customers, "products": products,
            "orders": orders, "deliveries": deliveries, "fact": fact,
        }
        kpis = compute_kpis_pandas(tables)
        assert kpis["total_revenue"] == 0.0
        assert kpis["cancellation_rate"] == pytest.approx(1.0)


# ─────────────────────────────────────────────────────────────────────────────
# SQL KPI engine tests
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeKpisSql:
    def test_total_orders(self, in_memory_db):
        kpis = compute_kpis_sql(in_memory_db)
        assert kpis["total_orders"] == 5

    def test_total_revenue(self, in_memory_db):
        kpis = compute_kpis_sql(in_memory_db)
        assert kpis["total_revenue"] == pytest.approx(21000.0, abs=1.0)

    def test_on_time_delivery_rate(self, in_memory_db):
        kpis = compute_kpis_sql(in_memory_db)
        assert kpis["on_time_delivery_rate"] == pytest.approx(0.75, abs=0.001)

    def test_cancellation_rate(self, in_memory_db):
        kpis = compute_kpis_sql(in_memory_db)
        assert kpis["cancellation_rate"] == pytest.approx(0.2, abs=0.001)

    def test_top_products_returns_dataframe(self, in_memory_db):
        kpis = compute_kpis_sql(in_memory_db)
        assert isinstance(kpis["top_products"], pd.DataFrame)
        assert len(kpis["top_products"]) > 0

    def test_monthly_revenue_returns_dataframe(self, in_memory_db):
        kpis = compute_kpis_sql(in_memory_db)
        assert isinstance(kpis["monthly_revenue"], pd.DataFrame)
        assert "year_month" in kpis["monthly_revenue"].columns
        assert "total_revenue" in kpis["monthly_revenue"].columns

    def test_top_segments_returns_dataframe(self, in_memory_db):
        kpis = compute_kpis_sql(in_memory_db)
        assert isinstance(kpis["top_segments"], pd.DataFrame)
        assert "segment" in kpis["top_segments"].columns


# ─────────────────────────────────────────────────────────────────────────────
# Cross-validation: pandas vs SQL engines
# ─────────────────────────────────────────────────────────────────────────────

class TestCrossValidation:
    TOLERANCE = 0.02  # 2% relative tolerance

    def _rel_diff(self, a: float, b: float) -> float:
        denom = max(abs(a), abs(b), 1e-9)
        return abs(a - b) / denom

    def test_total_orders_agree(self, standard_tables, in_memory_db):
        pd_kpis = compute_kpis_pandas(standard_tables)
        sql_kpis = compute_kpis_sql(in_memory_db)
        assert pd_kpis["total_orders"] == sql_kpis["total_orders"]

    def test_total_revenue_agree(self, standard_tables, in_memory_db):
        pd_kpis = compute_kpis_pandas(standard_tables)
        sql_kpis = compute_kpis_sql(in_memory_db)
        diff = self._rel_diff(pd_kpis["total_revenue"], sql_kpis["total_revenue"])
        assert diff < self.TOLERANCE, f"Revenue diverges: pd={pd_kpis['total_revenue']}, sql={sql_kpis['total_revenue']}"

    def test_cancellation_rate_agree(self, standard_tables, in_memory_db):
        pd_kpis = compute_kpis_pandas(standard_tables)
        sql_kpis = compute_kpis_sql(in_memory_db)
        diff = self._rel_diff(pd_kpis["cancellation_rate"], sql_kpis["cancellation_rate"])
        assert diff < self.TOLERANCE

    def test_on_time_rate_agree(self, standard_tables, in_memory_db):
        pd_kpis = compute_kpis_pandas(standard_tables)
        sql_kpis = compute_kpis_sql(in_memory_db)
        diff = self._rel_diff(
            pd_kpis["on_time_delivery_rate"],
            sql_kpis["on_time_delivery_rate"],
        )
        assert diff < self.TOLERANCE
