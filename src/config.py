"""
config.py
---------
Central configuration for the Automated Reporting Pipeline.

All path references and schema contracts live here so every module
imports from a single source of truth instead of hard-coding values.
"""

from pathlib import Path

# ── Root paths ─────────────────────────────────────────────────────────────────
BASE_DIR: Path = Path(__file__).parent.parent
DATA_DIR: Path = BASE_DIR / "data"
RAW_DIR: Path = DATA_DIR / "raw"
PROCESSED_DIR: Path = DATA_DIR / "processed"
OUTPUT_DIR: Path = DATA_DIR / "output"
SQL_DIR: Path = BASE_DIR / "sql"

# ── Database ───────────────────────────────────────────────────────────────────
DB_PATH: Path = DATA_DIR / "pipeline.duckdb"

# ── Source file names ──────────────────────────────────────────────────────────
SOURCE_FILES: dict[str, str] = {
    "orders": "orders.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "deliveries": "deliveries.csv",
}

# ── Expected columns per source (schema contract) ─────────────────────────────
EXPECTED_COLUMNS: dict[str, list[str]] = {
    "orders": [
        "order_id", "customer_id", "product_id",
        "order_date", "quantity", "unit_price", "order_status",
    ],
    "customers": [
        "customer_id", "customer_name", "segment",
        "city", "country", "signup_date",
    ],
    "products": [
        "product_id", "product_name", "category", "unit_cost",
    ],
    "deliveries": [
        "delivery_id", "order_id", "carrier",
        "shipped_date", "delivered_date", "delivery_status",
    ],
}

# ── Domain value sets ──────────────────────────────────────────────────────────
VALID_ORDER_STATUSES: set[str] = {"completed", "cancelled", "pending", "returned"}
VALID_DELIVERY_STATUSES: set[str] = {"on_time", "late", "failed"}

# ── Reporting ──────────────────────────────────────────────────────────────────
REPORT_TITLE: str = "Automated Business Performance Report"
TOP_N_PRODUCTS: int = 5
TOP_N_SEGMENTS: int = 5

# ── Output file names ──────────────────────────────────────────────────────────
KPI_CSV_NAME: str = "kpi_summary.csv"
KPI_EXCEL_NAME: str = "kpi_summary.xlsx"
REPORT_MD_NAME: str = "report.md"
