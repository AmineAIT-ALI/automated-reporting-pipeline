"""
utils.py
--------
Cross-cutting utilities: logging setup, directory helpers, and formatters.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timezone


# ── Logging ────────────────────────────────────────────────────────────────────

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure and return the root pipeline logger."""
    logger = logging.getLogger("pipeline")
    if logger.handlers:
        return logger  # already configured (e.g. during tests)

    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_logger(name: str) -> logging.Logger:
    """Return a child logger scoped to *name*."""
    return logging.getLogger(f"pipeline.{name}")


# ── Directory helpers ──────────────────────────────────────────────────────────

def ensure_directories(*paths: Path) -> None:
    """Create each directory (and any missing parents) if it does not already exist."""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


# ── Formatting helpers ─────────────────────────────────────────────────────────

def fmt_currency(value: float) -> str:
    """Format *value* as a EUR currency string with two decimal places (e.g. '€1,234.56')."""
    return f"€{value:,.2f}"


def fmt_percent(value: float) -> str:
    """Format a ratio in [0, 1] as a percentage string with one decimal place (e.g. '83.1%')."""
    return f"{value * 100:.1f}%"


def fmt_number(value: float | int) -> str:
    """Format *value* with a thousands separator.

    Floats are rendered with two decimal places; integers without decimals.
    """
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}"


def now_str() -> str:
    """Return the current UTC datetime as a human-readable string (seconds precision)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def now_filename_str() -> str:
    """Return the current UTC datetime as a filename-safe string (e.g. '20260313_142500')."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# ── Markdown table helper ──────────────────────────────────────────────────────

def md_table(headers: list[str], rows: list[list[str]]) -> str:
    """Build a GitHub-flavoured Markdown table from *headers* and *rows*.

    Column widths are auto-fitted to the widest cell in each column.
    """
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    def _row(cells: list[str]) -> str:
        """Format a single table row with left-aligned, padded cells."""
        return "| " + " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(cells)) + " |"

    # The separator line between the header and the data rows.
    separator = "| " + " | ".join("-" * w for w in col_widths) + " |"
    lines = [_row(headers), separator] + [_row(r) for r in rows]
    return "\n".join(lines)
