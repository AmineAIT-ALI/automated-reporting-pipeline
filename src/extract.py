"""
extract.py
----------
Extract layer: load raw CSV sources, validate schema contracts.

Each loader performs:
  1. File existence check
  2. CSV parsing with consistent dtypes-as-strings (transform layer handles casting)
  3. Column presence validation against the schema contract in config.py
"""

from pathlib import Path

import pandas as pd

from src.config import EXPECTED_COLUMNS, RAW_DIR, SOURCE_FILES
from src.utils import get_logger

logger = get_logger("extract")


# ── Low-level loader ───────────────────────────────────────────────────────────

def load_csv(path: Path, source_name: str) -> pd.DataFrame:
    """
    Read *path* as CSV, keep all columns as strings initially, and assert
    that every expected column for *source_name* is present.

    Raises:
        FileNotFoundError: if the file does not exist.
        ValueError: if required columns are missing.
    """
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    logger.info(f"Reading {source_name} from {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    # Replace empty strings with actual NaN so downstream cleaning is uniform
    df = df.replace("", pd.NA)

    expected = EXPECTED_COLUMNS.get(source_name, [])
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(
            f"[{source_name}] Missing expected columns: {sorted(missing)}"
        )

    extra = set(df.columns) - set(expected)
    if extra:
        logger.warning(f"[{source_name}] Unexpected extra columns (ignored): {sorted(extra)}")
        df = df[expected]  # keep only contracted columns

    logger.info(f"[{source_name}] Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


# ── Source-specific loaders ────────────────────────────────────────────────────

def load_orders(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    return load_csv(raw_dir / SOURCE_FILES["orders"], "orders")


def load_customers(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    return load_csv(raw_dir / SOURCE_FILES["customers"], "customers")


def load_products(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    return load_csv(raw_dir / SOURCE_FILES["products"], "products")


def load_deliveries(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    return load_csv(raw_dir / SOURCE_FILES["deliveries"], "deliveries")


# ── Bulk loader ────────────────────────────────────────────────────────────────

def load_all_sources(raw_dir: Path = RAW_DIR) -> dict[str, pd.DataFrame]:
    """
    Load all four source tables and return them as a dict keyed by table name.

    This is the primary entry-point for the Extract step.
    """
    logger.info("=== EXTRACT step started ===")
    sources = {
        "orders": load_orders(raw_dir),
        "customers": load_customers(raw_dir),
        "products": load_products(raw_dir),
        "deliveries": load_deliveries(raw_dir),
    }
    logger.info(
        "=== EXTRACT step completed — "
        + ", ".join(f"{k}: {len(v):,} rows" for k, v in sources.items())
        + " ==="
    )
    return sources
