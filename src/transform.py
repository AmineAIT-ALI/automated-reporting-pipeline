"""
transform.py
------------
Transform layer: clean, normalise, validate, and join raw source tables.

Responsibilities per table:
  - Deduplicate on natural keys
  - Cast columns to proper types (dates, numerics)
  - Enforce domain constraints (drop/flag invalid rows)
  - Handle nulls according to business rules
  - Produce a denormalised fact table for KPI computation

All cleaning decisions are logged so the pipeline is fully auditable.
"""

from pathlib import Path

import pandas as pd
import numpy as np

from src.config import VALID_ORDER_STATUSES, VALID_DELIVERY_STATUSES
from src.utils import get_logger

logger = get_logger("transform")


# ── Generic helpers ────────────────────────────────────────────────────────────

def _log_drop(label: str, before: int, after: int, reason: str) -> None:
    """Emit a warning when rows are dropped, stating the count and the reason."""
    dropped = before - after
    if dropped:
        logger.warning(f"[{label}] Dropped {dropped:,} rows — {reason}")


def _cast_date(series: pd.Series, col: str, source: str) -> pd.Series:
    """Parse a string Series as dates (YYYY-MM-DD format).

    Values that cannot be parsed are coerced to NaT and logged as warnings.
    """
    parsed = pd.to_datetime(series, errors="coerce", format="%Y-%m-%d")
    n_invalid = parsed.isna().sum() - series.isna().sum()
    if n_invalid > 0:
        logger.warning(f"[{source}] {n_invalid} unparseable dates in '{col}' → set to NaT")
    return parsed


def _cast_float(series: pd.Series, col: str, source: str) -> pd.Series:
    """Coerce a string Series to float64.

    Non-numeric values are set to NaN and logged as warnings.
    """
    casted = pd.to_numeric(series, errors="coerce")
    n_invalid = casted.isna().sum() - series.isna().sum()
    if n_invalid > 0:
        logger.warning(f"[{source}] {n_invalid} non-numeric values in '{col}' → set to NaN")
    return casted


# ── Table-level cleaners ───────────────────────────────────────────────────────

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the customers table.
    - Dedup on customer_id (keep first occurrence)
    - Drop rows with null customer_id or customer_name
    - Normalise segment to title-case, drop unknown segments
    - Parse signup_date
    """
    label = "customers"
    n0 = len(df)

    # Deduplicate on primary key, keeping the first occurrence.
    df = df.drop_duplicates(subset="customer_id", keep="first")
    _log_drop(label, n0, len(df), "duplicate customer_id")
    n0 = len(df)

    # Drop rows missing fields required for joining and identification.
    df = df.dropna(subset=["customer_id", "customer_name"])
    _log_drop(label, n0, len(df), "null customer_id or customer_name")
    n0 = len(df)

    # Normalise string fields: strip whitespace and apply title case where appropriate.
    df["customer_id"] = df["customer_id"].str.strip()
    df["customer_name"] = df["customer_name"].str.strip().str.title()
    # Segment is left as-is to preserve known acronyms (e.g. SMB stays SMB, not Smb).
    df["segment"] = df["segment"].str.strip()
    df["city"] = df["city"].str.strip().str.title()
    df["country"] = df["country"].str.strip().str.title()

    # Parse signup_date; invalid or missing values become NaT.
    df["signup_date"] = _cast_date(df["signup_date"], "signup_date", label)

    logger.info(f"[{label}] Clean output: {len(df):,} rows")
    return df.reset_index(drop=True)


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the products table.
    - Dedup on product_id
    - Drop rows with null product_id or product_name
    - Cast unit_cost to float; drop rows where cost <= 0
    """
    label = "products"
    n0 = len(df)

    df = df.drop_duplicates(subset="product_id", keep="first")
    _log_drop(label, n0, len(df), "duplicate product_id")
    n0 = len(df)

    df = df.dropna(subset=["product_id", "product_name"])
    _log_drop(label, n0, len(df), "null product_id or product_name")
    n0 = len(df)

    df["product_id"] = df["product_id"].str.strip()
    df["product_name"] = df["product_name"].str.strip()
    df["category"] = df["category"].str.strip().str.title()
    df["unit_cost"] = _cast_float(df["unit_cost"], "unit_cost", label)

    # Business rule: a product with zero or negative cost is considered invalid data.
    before = len(df)
    df = df[df["unit_cost"] > 0]
    _log_drop(label, before, len(df), "unit_cost <= 0")

    logger.info(f"[{label}] Clean output: {len(df):,} rows")
    return df.reset_index(drop=True)


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the orders table.
    - Dedup on order_id
    - Parse order_date; drop rows with null date or order_id
    - Cast quantity (int) and unit_price (float)
    - Drop rows with quantity <= 0
    - Impute missing unit_price with median per product
    - Enforce valid order_status values
    """
    label = "orders"
    n0 = len(df)

    df = df.drop_duplicates(subset="order_id", keep="first")
    _log_drop(label, n0, len(df), "duplicate order_id")
    n0 = len(df)

    df = df.dropna(subset=["order_id"])
    _log_drop(label, n0, len(df), "null order_id")
    n0 = len(df)

    df["order_id"] = df["order_id"].str.strip()
    df["customer_id"] = df["customer_id"].str.strip()
    df["product_id"] = df["product_id"].str.strip()

    df["order_date"] = _cast_date(df["order_date"], "order_date", label)
    before = len(df)
    df = df.dropna(subset=["order_date"])
    _log_drop(label, before, len(df), "unparseable order_date")
    n0 = len(df)

    # Cast quantity to integer and unit_price to float.
    df["quantity"] = _cast_float(df["quantity"], "quantity", label).astype("Int64")
    df["unit_price"] = _cast_float(df["unit_price"], "unit_price", label)

    # Drop rows where quantity is zero or negative (no economic value).
    before = len(df)
    df = df[df["quantity"] > 0]
    _log_drop(label, before, len(df), "quantity <= 0")

    # Impute missing unit_price values using the per-product median price.
    n_null_price = df["unit_price"].isna().sum()
    if n_null_price > 0:
        median_prices = df.groupby("product_id")["unit_price"].transform("median")
        global_median = df["unit_price"].median()
        df["unit_price"] = df["unit_price"].fillna(median_prices).fillna(global_median)
        logger.info(f"[{label}] Imputed {n_null_price} missing unit_price values with product median")

    # Normalise order_status to lowercase and enforce the domain value set.
    df["order_status"] = df["order_status"].str.strip().str.lower()
    before = len(df)
    df = df[df["order_status"].isin(VALID_ORDER_STATUSES)]
    _log_drop(label, before, len(df), f"order_status not in {VALID_ORDER_STATUSES}")

    # Compute revenue as quantity × unit_price; rounded to 2 decimal places.
    df["revenue"] = (df["quantity"] * df["unit_price"]).round(2)

    logger.info(f"[{label}] Clean output: {len(df):,} rows")
    return df.reset_index(drop=True)


def clean_deliveries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the deliveries table.
    - Dedup on delivery_id
    - Parse shipped_date and delivered_date
    - Enforce valid delivery_status values
    - Compute delivery_delay_days (delivered - shipped)
    """
    label = "deliveries"
    n0 = len(df)

    df = df.drop_duplicates(subset="delivery_id", keep="first")
    _log_drop(label, n0, len(df), "duplicate delivery_id")
    n0 = len(df)

    df = df.dropna(subset=["delivery_id", "order_id"])
    _log_drop(label, n0, len(df), "null delivery_id or order_id")
    n0 = len(df)

    df["delivery_id"] = df["delivery_id"].str.strip()
    df["order_id"] = df["order_id"].str.strip()
    df["carrier"] = df["carrier"].str.strip()

    df["shipped_date"] = _cast_date(df["shipped_date"], "shipped_date", label)
    df["delivered_date"] = _cast_date(df["delivered_date"], "delivered_date", label)

    df["delivery_status"] = df["delivery_status"].str.strip().str.lower()
    before = len(df)
    df = df[df["delivery_status"].isin(VALID_DELIVERY_STATUSES)]
    _log_drop(label, before, len(df), f"delivery_status not in {VALID_DELIVERY_STATUSES}")

    # Compute delivery delay as the number of calendar days between shipment and delivery.
    df["delivery_delay_days"] = (df["delivered_date"] - df["shipped_date"]).dt.days
    # Negative delay values indicate a data entry error and are set to NaN.
    invalid_delay = df["delivery_delay_days"] < 0
    if invalid_delay.any():
        logger.warning(f"[{label}] {invalid_delay.sum()} negative delivery delays → set to NaN")
        df.loc[invalid_delay, "delivery_delay_days"] = np.nan

    logger.info(f"[{label}] Clean output: {len(df):,} rows")
    return df.reset_index(drop=True)


# ── Fact table builder ─────────────────────────────────────────────────────────

def build_fact_table(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    deliveries: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join clean tables into a single denormalised fact table.

    Schema:
        order_id, order_date, order_status, revenue,
        quantity, unit_price,
        customer_id, customer_name, segment, city, country,
        product_id, product_name, category,
        delivery_id, delivery_status, delivery_delay_days,
        shipped_date, delivered_date, carrier,
        year_month (str 'YYYY-MM')
    """
    fact = (
        orders
        .merge(customers[["customer_id", "customer_name", "segment", "city", "country"]],
               on="customer_id", how="left")
        .merge(products[["product_id", "product_name", "category"]],
               on="product_id", how="left")
        .merge(deliveries[["order_id", "delivery_id", "delivery_status",
                            "delivery_delay_days", "shipped_date", "delivered_date", "carrier"]],
               on="order_id", how="left")
    )

    fact["year_month"] = fact["order_date"].dt.to_period("M").astype(str)

    logger.info(f"[fact_table] Built with {len(fact):,} rows × {len(fact.columns)} columns")
    return fact.reset_index(drop=True)


# ── Pipeline entry-point ───────────────────────────────────────────────────────

def transform_all(sources: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Apply all cleaning steps and build the fact table.

    Returns a dict with keys:
        customers, products, orders, deliveries, fact
    """
    logger.info("=== TRANSFORM step started ===")

    customers = clean_customers(sources["customers"])
    products = clean_products(sources["products"])
    orders = clean_orders(sources["orders"])
    deliveries = clean_deliveries(sources["deliveries"])
    fact = build_fact_table(orders, customers, products, deliveries)

    tables = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "deliveries": deliveries,
        "fact": fact,
    }

    logger.info(
        "=== TRANSFORM step completed — "
        + ", ".join(f"{k}: {len(v):,} rows" for k, v in tables.items())
        + " ==="
    )
    return tables
