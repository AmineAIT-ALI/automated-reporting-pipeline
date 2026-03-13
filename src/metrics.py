"""
metrics.py
----------
KPI computation layer — dual-engine approach.

KPIs are computed in two complementary ways:
  1. Pandas — for flexibility, testability, and quick prototyping
  2. SQL   — for auditability, performance on large datasets, and BI alignment

Both engines produce the same results; the SQL engine is the authoritative
source for the final report. Pandas results are used for unit testing.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd

from src.config import TOP_N_PRODUCTS, TOP_N_SEGMENTS
from src.utils import get_logger

logger = get_logger("metrics")

KPIResult = dict[str, Any]


# ── Pandas engine ──────────────────────────────────────────────────────────────

def compute_kpis_pandas(tables: dict[str, pd.DataFrame]) -> KPIResult:
    """
    Compute KPIs directly from clean DataFrames.
    Used for unit testing and cross-validation.
    """
    fact = tables["fact"]
    orders = tables["orders"]
    customers = tables["customers"]
    deliveries = tables["deliveries"]

    completed = fact[fact["order_status"] == "completed"]
    cancelled = fact[fact["order_status"] == "cancelled"]

    # ── Scalar KPIs ────────────────────────────────────────────────────────────
    total_orders = len(orders)
    total_revenue = completed["revenue"].sum()
    average_order_value = completed["revenue"].mean() if len(completed) > 0 else 0.0
    total_customers = customers["customer_id"].nunique()
    cancellation_rate = len(cancelled) / total_orders if total_orders > 0 else 0.0

    # ── Delivery KPIs ──────────────────────────────────────────────────────────
    delivered = deliveries[deliveries["delivery_status"].isin(["on_time", "late"])]
    on_time = deliveries[deliveries["delivery_status"] == "on_time"]
    late = deliveries[deliveries["delivery_status"] == "late"]

    on_time_delivery_rate = len(on_time) / len(delivered) if len(delivered) > 0 else 0.0
    late_delivery_rate = len(late) / len(delivered) if len(delivered) > 0 else 0.0
    average_delivery_delay_days = (
        deliveries["delivery_delay_days"].mean()
        if "delivery_delay_days" in deliveries.columns
        else 0.0
    )

    # ── Top products ───────────────────────────────────────────────────────────
    top_products = (
        completed.groupby(["product_id", "product_name"], as_index=False)["revenue"]
        .sum()
        .rename(columns={"revenue": "total_revenue"})
        .sort_values("total_revenue", ascending=False)
        .head(TOP_N_PRODUCTS)
        .reset_index(drop=True)
    )

    # ── Monthly revenue ────────────────────────────────────────────────────────
    monthly_revenue = (
        completed.groupby("year_month", as_index=False)["revenue"]
        .sum()
        .sort_values("year_month")
        .reset_index(drop=True)
    )
    monthly_revenue = monthly_revenue.rename(columns={"revenue": "total_revenue"})

    # ── Top customer segments ──────────────────────────────────────────────────
    top_segments = (
        completed.groupby("segment", as_index=False)
        .agg(
            total_revenue=("revenue", "sum"),
            order_count=("order_id", "count"),
        )
        .sort_values("total_revenue", ascending=False)
        .head(TOP_N_SEGMENTS)
        .reset_index(drop=True)
    )

    # ── Revenue by category ────────────────────────────────────────────────────
    revenue_by_category = (
        completed.groupby("category", as_index=False)["revenue"]
        .sum()
        .sort_values("revenue", ascending=False)
        .reset_index(drop=True)
    )

    # ── Revenue by country ─────────────────────────────────────────────────────
    revenue_by_country = (
        completed[completed["country"].notna()]
        .groupby("country", as_index=False)
        .agg(total_revenue=("revenue", "sum"), order_count=("order_id", "count"))
        .sort_values("total_revenue", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    # ── Return rate ────────────────────────────────────────────────────────────
    returned = fact[fact["order_status"] == "returned"]
    return_rate = len(returned) / total_orders if total_orders > 0 else 0.0

    kpis = {
        "total_orders": int(total_orders),
        "total_revenue": round(float(total_revenue), 2),
        "average_order_value": round(float(average_order_value), 2),
        "total_customers": int(total_customers),
        "on_time_delivery_rate": round(float(on_time_delivery_rate), 4),
        "late_delivery_rate": round(float(late_delivery_rate), 4),
        "average_delivery_delay_days": round(float(average_delivery_delay_days), 2)
            if pd.notna(average_delivery_delay_days) else 0.0,
        "cancellation_rate": round(float(cancellation_rate), 4),
        "return_rate": round(float(return_rate), 4),
        "top_products": top_products,
        "monthly_revenue": monthly_revenue,
        "top_segments": top_segments,
        "revenue_by_category": revenue_by_category,
        "revenue_by_country": revenue_by_country,
    }

    logger.info(
        f"[pandas] Scalar KPIs — "
        f"orders={kpis['total_orders']:,} | "
        f"revenue=€{kpis['total_revenue']:,.2f} | "
        f"aov=€{kpis['average_order_value']:,.2f} | "
        f"on_time={kpis['on_time_delivery_rate']:.1%} | "
        f"cancel={kpis['cancellation_rate']:.1%}"
    )
    return kpis


# ── SQL engine ─────────────────────────────────────────────────────────────────

def compute_kpis_sql(conn: duckdb.DuckDBPyConnection) -> KPIResult:
    """
    Compute KPIs via SQL queries against the DuckDB fact table.
    This is the authoritative engine for the final report.
    """

    def q(sql: str) -> pd.DataFrame:
        """Execute *sql* and return the result as a DataFrame."""
        return conn.execute(sql).df()

    def scalar(sql: str):
        """Execute *sql* and return the first column of the first row, or None."""
        result = conn.execute(sql).fetchone()
        return result[0] if result else None

    logger.info("[sql] Computing KPIs from DuckDB fact table...")

    # ── Scalar KPIs ────────────────────────────────────────────────────────────
    total_orders = scalar("SELECT COUNT(DISTINCT order_id) FROM orders")

    total_revenue = scalar("""
        SELECT ROUND(SUM(revenue), 2)
        FROM fact
        WHERE order_status = 'completed'
    """)

    average_order_value = scalar("""
        SELECT ROUND(AVG(revenue), 2)
        FROM fact
        WHERE order_status = 'completed'
    """)

    total_customers = scalar("SELECT COUNT(DISTINCT customer_id) FROM customers")

    cancellation_rate = scalar("""
        SELECT ROUND(
            SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END)::DOUBLE
            / NULLIF(COUNT(*), 0), 4
        )
        FROM fact
    """)

    return_rate = scalar("""
        SELECT ROUND(
            SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END)::DOUBLE
            / NULLIF(COUNT(*), 0), 4
        )
        FROM fact
    """)

    on_time_delivery_rate = scalar("""
        SELECT ROUND(
            SUM(CASE WHEN delivery_status = 'on_time' THEN 1 ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN delivery_status IN ('on_time','late') THEN 1 ELSE 0 END), 0), 4
        )
        FROM deliveries
    """)

    late_delivery_rate = scalar("""
        SELECT ROUND(
            SUM(CASE WHEN delivery_status = 'late' THEN 1 ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN delivery_status IN ('on_time','late') THEN 1 ELSE 0 END), 0), 4
        )
        FROM deliveries
    """)

    average_delivery_delay_days = scalar("""
        SELECT ROUND(AVG(delivery_delay_days), 2)
        FROM deliveries
        WHERE delivery_delay_days IS NOT NULL
    """)

    # ── Tabular KPIs ───────────────────────────────────────────────────────────
    top_products = q(f"""
        SELECT
            product_id,
            product_name,
            ROUND(SUM(revenue), 2)   AS total_revenue,
            SUM(quantity)            AS total_units_sold
        FROM fact
        WHERE order_status = 'completed'
        GROUP BY product_id, product_name
        ORDER BY total_revenue DESC
        LIMIT {TOP_N_PRODUCTS}
    """)

    monthly_revenue = q("""
        SELECT
            year_month,
            ROUND(SUM(revenue), 2)  AS total_revenue,
            COUNT(order_id)         AS order_count
        FROM fact
        WHERE order_status = 'completed'
        GROUP BY year_month
        ORDER BY year_month
    """)

    top_segments = q(f"""
        SELECT
            segment,
            ROUND(SUM(revenue), 2)  AS total_revenue,
            COUNT(order_id)         AS order_count,
            COUNT(DISTINCT customer_id) AS unique_customers
        FROM fact
        WHERE order_status = 'completed'
          AND segment IS NOT NULL
        GROUP BY segment
        ORDER BY total_revenue DESC
        LIMIT {TOP_N_SEGMENTS}
    """)

    revenue_by_category = q("""
        SELECT
            category,
            ROUND(SUM(revenue), 2)  AS total_revenue,
            SUM(quantity)           AS total_units_sold
        FROM fact
        WHERE order_status = 'completed'
          AND category IS NOT NULL
        GROUP BY category
        ORDER BY total_revenue DESC
    """)

    revenue_by_country = q("""
        SELECT
            country,
            ROUND(SUM(revenue), 2)  AS total_revenue,
            COUNT(order_id)         AS order_count
        FROM fact
        WHERE order_status = 'completed'
          AND country IS NOT NULL
        GROUP BY country
        ORDER BY total_revenue DESC
        LIMIT 10
    """)

    kpis = {
        "total_orders": int(total_orders or 0),
        "total_revenue": float(total_revenue or 0),
        "average_order_value": float(average_order_value or 0),
        "total_customers": int(total_customers or 0),
        "on_time_delivery_rate": float(on_time_delivery_rate or 0),
        "late_delivery_rate": float(late_delivery_rate or 0),
        "average_delivery_delay_days": float(average_delivery_delay_days or 0),
        "cancellation_rate": float(cancellation_rate or 0),
        "return_rate": float(return_rate or 0),
        "top_products": top_products,
        "monthly_revenue": monthly_revenue,
        "top_segments": top_segments,
        "revenue_by_category": revenue_by_category,
        "revenue_by_country": revenue_by_country,
    }

    logger.info(
        f"[sql] Scalar KPIs — "
        f"orders={kpis['total_orders']:,} | "
        f"revenue=€{kpis['total_revenue']:,.2f} | "
        f"aov=€{kpis['average_order_value']:,.2f} | "
        f"on_time={kpis['on_time_delivery_rate']:.1%} | "
        f"cancel={kpis['cancellation_rate']:.1%}"
    )
    return kpis


# ── Console summary ────────────────────────────────────────────────────────────

def print_kpi_summary(kpis: KPIResult) -> None:
    """Print a formatted KPI dashboard to stdout."""
    from src.utils import fmt_currency, fmt_percent, fmt_number

    width = 60
    line = "─" * width

    print(f"\n{'═' * width}")
    print(f"  BUSINESS PERFORMANCE KPI DASHBOARD")
    print(f"{'═' * width}")

    print(f"\n  {'COMMERCIAL METRICS':}")
    print(f"  {line}")
    print(f"  {'Total Orders':<35} {fmt_number(kpis['total_orders']):>12}")
    print(f"  {'Total Revenue':<35} {fmt_currency(kpis['total_revenue']):>12}")
    print(f"  {'Average Order Value':<35} {fmt_currency(kpis['average_order_value']):>12}")
    print(f"  {'Total Customers':<35} {fmt_number(kpis['total_customers']):>12}")
    print(f"  {'Cancellation Rate':<35} {fmt_percent(kpis['cancellation_rate']):>12}")
    print(f"  {'Return Rate':<35} {fmt_percent(kpis['return_rate']):>12}")

    print(f"\n  {'DELIVERY METRICS':}")
    print(f"  {line}")
    print(f"  {'On-Time Delivery Rate':<35} {fmt_percent(kpis['on_time_delivery_rate']):>12}")
    print(f"  {'Late Delivery Rate':<35} {fmt_percent(kpis['late_delivery_rate']):>12}")
    print(f"  {'Avg Delivery Delay (days)':<35} {kpis['average_delivery_delay_days']:>11.1f}d")

    if "top_products" in kpis and not kpis["top_products"].empty:
        print(f"\n  TOP {TOP_N_PRODUCTS} PRODUCTS BY REVENUE")
        print(f"  {line}")
        for _, row in kpis["top_products"].iterrows():
            name = str(row.get("product_name", "N/A"))[:30]
            rev = float(row.get("total_revenue", 0))
            print(f"  {name:<35} {fmt_currency(rev):>12}")

    if "top_segments" in kpis and not kpis["top_segments"].empty:
        print(f"\n  {'TOP CUSTOMER SEGMENTS':}")
        print(f"  {line}")
        for _, row in kpis["top_segments"].iterrows():
            seg = str(row.get("segment", "N/A"))
            rev = float(row.get("total_revenue", 0))
            print(f"  {seg:<35} {fmt_currency(rev):>12}")

    print(f"\n{'═' * width}\n")
