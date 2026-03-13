"""
pipeline.py
-----------
Orchestration layer: wire Extract → Transform → Load → Metrics → Report.

This module contains a single public function `run_pipeline()` that
executes the full pipeline end-to-end and returns a structured result dict.

Architecture:
  1. EXTRACT   — load raw CSVs, validate schema
  2. TRANSFORM — clean, normalise, build fact table
  3. LOAD      — persist clean tables into DuckDB
  4. METRICS   — compute KPIs (SQL engine, cross-validated via pandas)
  5. REPORT    — generate CSV / Excel / Markdown outputs
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from src.config import DB_PATH, OUTPUT_DIR, PROCESSED_DIR, RAW_DIR, SQL_DIR
from src.extract import load_all_sources
from src.load import init_and_load
from src.metrics import compute_kpis_pandas, compute_kpis_sql, print_kpi_summary
from src.report import generate_all
from src.transform import transform_all
from src.utils import ensure_directories, get_logger, now_str

logger = get_logger("pipeline")


def run_pipeline(
    raw_dir: Path = RAW_DIR,
    output_dir: Path = OUTPUT_DIR,
    db_path: Path = DB_PATH,
    sql_dir: Path = SQL_DIR,
    save_processed: bool = True,
    print_summary: bool = True,
) -> dict[str, Any]:
    """
    Execute the full Automated Reporting Pipeline.

    Parameters
    ----------
    raw_dir : Path
        Directory containing source CSV files.
    output_dir : Path
        Directory where reports will be written.
    db_path : Path
        Path to the DuckDB file (created if absent).
    sql_dir : Path
        Directory containing SQL files (schema.sql, kpi_queries.sql, …).
    save_processed : bool
        If True, write clean DataFrames to data/processed/ for inspection.
    print_summary : bool
        If True, print the KPI dashboard to stdout after the run.

    Returns
    -------
    dict with keys:
        tables     — dict of clean DataFrames
        kpis       — dict of computed KPIs
        outputs    — dict of output file paths
        duration_s — total pipeline wall-clock time (seconds)
        run_at     — ISO timestamp of this run
    """
    run_at = now_str()
    t_start = time.perf_counter()

    ensure_directories(raw_dir, output_dir, PROCESSED_DIR)

    logger.info(f"{'=' * 60}")
    logger.info(f"  Automated Reporting Pipeline — run started at {run_at}")
    logger.info(f"{'=' * 60}")
    logger.info(f"  raw_dir    : {raw_dir}")
    logger.info(f"  output_dir : {output_dir}")
    logger.info(f"  db_path    : {db_path}")

    # ── Step 1: EXTRACT — load raw CSV files from disk ─────────────────────────
    t0 = time.perf_counter()
    sources = load_all_sources(raw_dir)
    logger.info(f"[timer] EXTRACT: {time.perf_counter() - t0:.2f}s")

    # ── Step 2: TRANSFORM — clean, normalise, and build the fact table ─────────
    t0 = time.perf_counter()
    tables = transform_all(sources)
    logger.info(f"[timer] TRANSFORM: {time.perf_counter() - t0:.2f}s")

    # Optionally persist processed DataFrames to Parquet for audit or downstream use.
    if save_processed:
        for name, df in tables.items():
            proc_path = PROCESSED_DIR / f"{name}.parquet"
            df.to_parquet(proc_path, index=False)
            logger.debug(f"Processed table saved: {proc_path}")
        logger.info(f"Processed tables saved to {PROCESSED_DIR}")

    # ── Step 3: LOAD — persist clean tables into DuckDB ────────────────────────
    t0 = time.perf_counter()
    conn = init_and_load(tables, db_path=db_path, sql_dir=sql_dir)
    logger.info(f"[timer] LOAD: {time.perf_counter() - t0:.2f}s")

    # ── Step 4: METRICS — compute KPIs via SQL (authoritative) and pandas ──────
    t0 = time.perf_counter()
    try:
        kpis_sql = compute_kpis_sql(conn)
        kpis_pandas = compute_kpis_pandas(tables)  # Used for cross-validation only.
    finally:
        # Always close the connection, even if an exception is raised.
        conn.close()
    logger.info(f"[timer] METRICS: {time.perf_counter() - t0:.2f}s")

    # Warn if scalar KPIs computed by both engines diverge beyond the tolerance threshold.
    _cross_validate(kpis_sql, kpis_pandas)

    # ── Step 5: REPORT — generate CSV, Excel, and Markdown outputs ─────────────
    t0 = time.perf_counter()
    outputs = generate_all(kpis_sql, output_dir)
    logger.info(f"[timer] REPORT: {time.perf_counter() - t0:.2f}s")

    duration = time.perf_counter() - t_start
    logger.info(f"{'=' * 60}")
    logger.info(f"  Pipeline completed in {duration:.2f}s")
    logger.info(f"  Outputs written to: {output_dir.resolve()}")
    logger.info(f"{'=' * 60}")

    if print_summary:
        print_kpi_summary(kpis_sql)

    return {
        "tables": tables,
        "kpis": kpis_sql,
        "outputs": outputs,
        "duration_s": round(duration, 3),
        "run_at": run_at,
    }


# ── Cross-validation helper ────────────────────────────────────────────────────

def _cross_validate(
    sql_kpis: dict[str, Any],
    pandas_kpis: dict[str, Any],
    tolerance: float = 0.01,
) -> None:
    """
    Warn if scalar KPIs computed by SQL and pandas diverge more than *tolerance*
    (relative difference). This catches bugs in either engine.
    """
    scalar_keys = [
        "total_orders", "total_revenue", "average_order_value",
        "on_time_delivery_rate", "cancellation_rate",
    ]
    for key in scalar_keys:
        sql_val = sql_kpis.get(key, 0) or 0
        pd_val = pandas_kpis.get(key, 0) or 0
        if sql_val == 0 and pd_val == 0:
            continue
        denominator = max(abs(sql_val), abs(pd_val))
        rel_diff = abs(sql_val - pd_val) / denominator
        if rel_diff > tolerance:
            logger.warning(
                f"[cross-validation] '{key}' diverges: SQL={sql_val} vs pandas={pd_val} "
                f"(diff={rel_diff:.2%})"
            )
        else:
            logger.debug(f"[cross-validation] '{key}' OK (diff={rel_diff:.4%})")
