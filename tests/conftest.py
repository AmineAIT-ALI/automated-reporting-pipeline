"""
conftest.py
-----------
Shared pytest fixtures for the Automated Reporting Pipeline test suite.
"""

import pandas as pd
import pytest


# ── Minimal raw DataFrames (string-typed, as returned by extract layer) ────────

@pytest.fixture
def raw_customers() -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": ["CUST001", "CUST002", "CUST003", "CUST001"],  # dup CUST001
        "customer_name": ["Alice Martin", "Bob Durand", "Carol Simon", "Alice Martin"],
        "segment": ["Enterprise", "SMB", "startup", "Enterprise"],  # lowercase "startup"
        "city": ["Paris", "Lyon", "Marseille", "Paris"],
        "country": ["France", "France", "France", "France"],
        "signup_date": ["2022-01-15", "2021-06-30", "not-a-date", "2022-01-15"],
    })


@pytest.fixture
def raw_products() -> pd.DataFrame:
    return pd.DataFrame({
        "product_id": ["PROD001", "PROD002", "PROD003"],
        "product_name": ["CRM Pro", "Server Rack", "Consulting Pack"],
        "category": ["Software", "Hardware", "Services"],
        "unit_cost": ["1200.00", "4500.00", "-50.00"],  # last one is invalid
    })


@pytest.fixture
def raw_orders() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": ["ORD001", "ORD002", "ORD003", "ORD004", "ORD005", "ORD001"],  # dup ORD001
        "customer_id": ["CUST001", "CUST002", "CUST001", "CUST003", "CUST002", "CUST001"],
        "product_id": ["PROD001", "PROD002", "PROD001", "PROD003", "PROD002", "PROD001"],
        "order_date": ["2023-01-10", "2023-02-14", "2023-03-05", "2023-04-20",
                       "not-a-date", "2023-01-10"],
        "quantity": ["2", "1", "-1", "3", "1", "2"],  # -1 is invalid
        "unit_price": ["1500.00", "5000.00", "1500.00", "", "5000.00", "1500.00"],  # null price
        "order_status": ["completed", "completed", "cancelled", "completed", "pending", "completed"],
    })


@pytest.fixture
def raw_deliveries() -> pd.DataFrame:
    return pd.DataFrame({
        "delivery_id": ["DEL001", "DEL002", "DEL003", "DEL001"],  # dup DEL001
        "order_id": ["ORD001", "ORD002", "ORD004", "ORD001"],
        "carrier": ["DHL", "FedEx", "UPS", "DHL"],
        "shipped_date": ["2023-01-11", "2023-02-15", "2023-04-21", "2023-01-11"],
        "delivered_date": ["2023-01-14", "2023-02-18", "2023-04-30", "2023-01-14"],
        "delivery_status": ["on_time", "on_time", "late", "on_time"],
    })


# ── Clean DataFrames (as produced by transform layer) ─────────────────────────

@pytest.fixture
def clean_customers() -> pd.DataFrame:
    from src.transform import clean_customers
    return clean_customers(pd.DataFrame({
        "customer_id": ["CUST001", "CUST002", "CUST003"],
        "customer_name": ["Alice Martin", "Bob Durand", "Carol Simon"],
        "segment": ["Enterprise", "SMB", "Startup"],
        "city": ["Paris", "Lyon", "Marseille"],
        "country": ["France", "France", "France"],
        "signup_date": ["2022-01-15", "2021-06-30", "2020-05-01"],
    }))


@pytest.fixture
def clean_products() -> pd.DataFrame:
    from src.transform import clean_products
    return clean_products(pd.DataFrame({
        "product_id": ["PROD001", "PROD002", "PROD003"],
        "product_name": ["CRM Pro", "Server Rack", "Consulting Pack"],
        "category": ["Software", "Hardware", "Services"],
        "unit_cost": ["1200.00", "4500.00", "2000.00"],
    }))


@pytest.fixture
def clean_orders() -> pd.DataFrame:
    from src.transform import clean_orders
    return clean_orders(pd.DataFrame({
        "order_id": ["ORD001", "ORD002", "ORD003", "ORD004"],
        "customer_id": ["CUST001", "CUST002", "CUST001", "CUST003"],
        "product_id": ["PROD001", "PROD002", "PROD001", "PROD003"],
        "order_date": ["2023-01-10", "2023-02-14", "2023-03-05", "2023-04-20"],
        "quantity": ["2", "1", "3", "2"],
        "unit_price": ["1500.00", "5000.00", "1500.00", "2500.00"],
        "order_status": ["completed", "completed", "cancelled", "completed"],
    }))


@pytest.fixture
def clean_deliveries() -> pd.DataFrame:
    from src.transform import clean_deliveries
    return clean_deliveries(pd.DataFrame({
        "delivery_id": ["DEL001", "DEL002", "DEL003"],
        "order_id": ["ORD001", "ORD002", "ORD004"],
        "carrier": ["DHL", "FedEx", "UPS"],
        "shipped_date": ["2023-01-11", "2023-02-15", "2023-04-21"],
        "delivered_date": ["2023-01-14", "2023-02-18", "2023-04-30"],
        "delivery_status": ["on_time", "on_time", "late"],
    }))
