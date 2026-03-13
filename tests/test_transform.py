"""
test_transform.py
-----------------
Unit tests for the transform layer (src/transform.py).

Coverage:
  - Deduplication on primary keys
  - Type casting (dates, numerics)
  - Business rule enforcement (negative quantities, invalid statuses, …)
  - Null handling and imputation
  - Derived columns (revenue, delivery_delay_days)
  - build_fact_table join integrity
"""

import pandas as pd
import pytest

from src.transform import (
    clean_customers,
    clean_deliveries,
    clean_orders,
    clean_products,
    build_fact_table,
    transform_all,
)


# ─────────────────────────────────────────────────────────────────────────────
# clean_customers
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanCustomers:
    def test_deduplication(self, raw_customers):
        result = clean_customers(raw_customers)
        # CUST001 appears twice — only one should remain
        assert result["customer_id"].duplicated().sum() == 0
        assert len(result) == 3

    def test_invalid_date_dropped(self, raw_customers):
        """Row with unparseable signup_date should keep the row but NaT the date."""
        result = clean_customers(raw_customers)
        carol_row = result[result["customer_id"] == "CUST003"]
        assert not carol_row.empty
        assert pd.isna(carol_row.iloc[0]["signup_date"])

    def test_segment_stripped(self, raw_customers):
        result = clean_customers(raw_customers)
        # Segment is stripped but casing is preserved (so "SMB" stays "SMB")
        carol_row = result[result["customer_id"] == "CUST003"]
        assert carol_row.iloc[0]["segment"] == "startup"  # raw value was lowercase

    def test_signup_date_is_datetime(self, raw_customers):
        result = clean_customers(raw_customers)
        assert pd.api.types.is_datetime64_any_dtype(result["signup_date"])

    def test_no_null_customer_id(self):
        df = pd.DataFrame({
            "customer_id": [None, "CUST001"],
            "customer_name": ["Ghost", "Alice"],
            "segment": ["SMB", "Enterprise"],
            "city": ["Paris", "Lyon"],
            "country": ["France", "France"],
            "signup_date": ["2022-01-01", "2022-01-01"],
        })
        result = clean_customers(df)
        assert result["customer_id"].notna().all()
        assert len(result) == 1


# ─────────────────────────────────────────────────────────────────────────────
# clean_products
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanProducts:
    def test_negative_cost_dropped(self, raw_products):
        result = clean_products(raw_products)
        assert (result["unit_cost"] > 0).all()

    def test_cost_is_float(self, raw_products):
        result = clean_products(raw_products)
        assert pd.api.types.is_float_dtype(result["unit_cost"])

    def test_deduplication(self):
        df = pd.DataFrame({
            "product_id": ["PROD001", "PROD001"],
            "product_name": ["CRM Pro", "CRM Pro"],
            "category": ["Software", "Software"],
            "unit_cost": ["1200.00", "1200.00"],
        })
        result = clean_products(df)
        assert len(result) == 1

    def test_category_title_case(self):
        df = pd.DataFrame({
            "product_id": ["PROD001"],
            "product_name": ["Tool"],
            "category": ["software"],
            "unit_cost": ["500.00"],
        })
        result = clean_products(df)
        assert result.iloc[0]["category"] == "Software"


# ─────────────────────────────────────────────────────────────────────────────
# clean_orders
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanOrders:
    def test_deduplication(self, raw_orders):
        result = clean_orders(raw_orders)
        assert result["order_id"].duplicated().sum() == 0

    def test_negative_quantity_dropped(self, raw_orders):
        result = clean_orders(raw_orders)
        assert (result["quantity"] > 0).all()

    def test_invalid_date_row_dropped(self, raw_orders):
        result = clean_orders(raw_orders)
        # ORD005 had "not-a-date" → should be dropped
        assert "ORD005" not in result["order_id"].values

    def test_null_price_imputed(self, raw_orders):
        result = clean_orders(raw_orders)
        assert result["unit_price"].notna().all()

    def test_revenue_derived_column(self, raw_orders):
        result = clean_orders(raw_orders)
        assert "revenue" in result.columns
        completed = result[result["order_status"] == "completed"]
        for _, row in completed.iterrows():
            expected = round(row["quantity"] * row["unit_price"], 2)
            assert abs(row["revenue"] - expected) < 0.01

    def test_order_date_is_datetime(self, raw_orders):
        result = clean_orders(raw_orders)
        assert pd.api.types.is_datetime64_any_dtype(result["order_date"])

    def test_invalid_status_dropped(self):
        df = pd.DataFrame({
            "order_id": ["ORD001", "ORD002"],
            "customer_id": ["C1", "C2"],
            "product_id": ["P1", "P2"],
            "order_date": ["2023-01-01", "2023-02-01"],
            "quantity": ["1", "1"],
            "unit_price": ["100.00", "200.00"],
            "order_status": ["completed", "UNKNOWN_STATUS"],
        })
        result = clean_orders(df)
        assert "ORD002" not in result["order_id"].values
        assert len(result) == 1


# ─────────────────────────────────────────────────────────────────────────────
# clean_deliveries
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanDeliveries:
    def test_deduplication(self, raw_deliveries):
        result = clean_deliveries(raw_deliveries)
        assert result["delivery_id"].duplicated().sum() == 0

    def test_delay_computed(self, raw_deliveries):
        result = clean_deliveries(raw_deliveries)
        assert "delivery_delay_days" in result.columns
        on_time_row = result[result["delivery_id"] == "DEL001"]
        # shipped 2023-01-11, delivered 2023-01-14 → 3 days
        assert on_time_row.iloc[0]["delivery_delay_days"] == 3

    def test_negative_delay_nullified(self):
        df = pd.DataFrame({
            "delivery_id": ["DEL001"],
            "order_id": ["ORD001"],
            "carrier": ["DHL"],
            "shipped_date": ["2023-03-10"],
            "delivered_date": ["2023-03-05"],  # before ship date → invalid
            "delivery_status": ["on_time"],
        })
        result = clean_deliveries(df)
        assert pd.isna(result.iloc[0]["delivery_delay_days"])

    def test_shipped_date_is_datetime(self, raw_deliveries):
        result = clean_deliveries(raw_deliveries)
        assert pd.api.types.is_datetime64_any_dtype(result["shipped_date"])


# ─────────────────────────────────────────────────────────────────────────────
# build_fact_table
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildFactTable:
    def test_column_presence(self, clean_customers, clean_products, clean_orders, clean_deliveries):
        fact = build_fact_table(clean_orders, clean_customers, clean_products, clean_deliveries)
        expected_cols = [
            "order_id", "customer_name", "segment", "product_name",
            "category", "year_month", "revenue",
        ]
        for col in expected_cols:
            assert col in fact.columns, f"Missing column: {col}"

    def test_year_month_format(self, clean_customers, clean_products, clean_orders, clean_deliveries):
        fact = build_fact_table(clean_orders, clean_customers, clean_products, clean_deliveries)
        # year_month should be 'YYYY-MM'
        assert fact["year_month"].str.match(r"\d{4}-\d{2}").all()

    def test_row_count_equals_orders(
        self, clean_customers, clean_products, clean_orders, clean_deliveries
    ):
        fact = build_fact_table(clean_orders, clean_customers, clean_products, clean_deliveries)
        # Left join on orders → fact should have same count as orders
        assert len(fact) == len(clean_orders)


# ─────────────────────────────────────────────────────────────────────────────
# transform_all (integration)
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformAll:
    def test_returns_all_keys(
        self, raw_customers, raw_products, raw_orders, raw_deliveries
    ):
        sources = {
            "customers": raw_customers,
            "products": raw_products,
            "orders": raw_orders,
            "deliveries": raw_deliveries,
        }
        tables = transform_all(sources)
        for key in ["customers", "products", "orders", "deliveries", "fact"]:
            assert key in tables

    def test_fact_not_empty(
        self, raw_customers, raw_products, raw_orders, raw_deliveries
    ):
        sources = {
            "customers": raw_customers,
            "products": raw_products,
            "orders": raw_orders,
            "deliveries": raw_deliveries,
        }
        tables = transform_all(sources)
        assert len(tables["fact"]) > 0
