"""
load.py
-------
Load layer: persist clean tables into a local DuckDB database.

DuckDB is used as the analytical store because:
  - Zero-configuration, file-based (no server needed)
  - Full SQL with window functions and ASOF joins
  - Native pandas integration (zero-copy reads)
  - Excellent performance for analytical aggregations

The schema is created from sql/schema.sql on first run; subsequent runs
drop and recreate tables to ensure idempotency.
"""

from pathlib import Path

import duckdb
import pandas as pd

from src.config import DB_PATH, SQL_DIR
from src.utils import get_logger

logger = get_logger("load")

# Explicit whitelist of valid table names.
# Used to prevent SQL injection when table names are interpolated into queries.
_ALLOWED_TABLES = frozenset(["customers", "products", "orders", "deliveries", "fact"])


# ── Connection ─────────────────────────────────────────────────────────────────

def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open (or create) a DuckDB database file and return the connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    logger.debug(f"Connected to DuckDB at {db_path}")
    return conn


# ── Schema creation ────────────────────────────────────────────────────────────

def create_schema(conn: duckdb.DuckDBPyConnection, sql_dir: Path = SQL_DIR) -> None:
    """Execute sql/schema.sql to (re)create all tables in DuckDB."""
    schema_path = sql_dir / "schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    logger.info(f"Executing schema from {schema_path}")
    sql = schema_path.read_text(encoding="utf-8")
    conn.execute(sql)
    logger.info("Schema created successfully")


# ── Data loading ───────────────────────────────────────────────────────────────

def _load_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
) -> None:
    """
    Insert *df* into *table_name* in DuckDB.

    Uses DuckDB's native 'INSERT INTO … SELECT * FROM df' pattern which
    avoids any intermediate serialisation step.
    """
    if table_name not in _ALLOWED_TABLES:
        raise ValueError(f"Unknown table: {table_name!r}")
    quoted = f'"{table_name}"'  # SQL standard identifier quoting.

    # Reorder DataFrame columns to match the DuckDB schema order.
    # INSERT INTO … SELECT * is positional, so column order must match exactly.
    schema_cols = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [table_name],
    ).df()["column_name"].tolist()
    df_ordered = df[[c for c in schema_cols if c in df.columns]]

    # Delete existing rows before reinserting to guarantee idempotent reloads.
    conn.execute(f"DELETE FROM {quoted}")
    conn.execute(f"INSERT INTO {quoted} SELECT * FROM df_ordered")
    row_count = conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
    logger.info(f"[load] {table_name}: {row_count:,} rows loaded")


def load_tables(
    conn: duckdb.DuckDBPyConnection,
    tables: dict[str, pd.DataFrame],
) -> None:
    """
    Load all clean DataFrames into DuckDB.

    Expected keys in *tables*: customers, products, orders, deliveries, fact.
    """
    logger.info("=== LOAD step started ===")

    load_order = ["customers", "products", "orders", "deliveries", "fact"]
    for name in load_order:
        if name not in tables:
            raise KeyError(f"Required table '{name}' not found in tables dict")
        _load_table(conn, name, tables[name])

    logger.info("=== LOAD step completed ===")


# ── Convenience: full load pipeline ───────────────────────────────────────────

def init_and_load(
    tables: dict[str, pd.DataFrame],
    db_path: Path = DB_PATH,
    sql_dir: Path = SQL_DIR,
) -> duckdb.DuckDBPyConnection:
    """
    One-call helper: open connection, create schema, load tables.

    Returns the open connection (caller must close it).
    """
    conn = get_connection(db_path)
    create_schema(conn, sql_dir)
    load_tables(conn, tables)
    return conn
