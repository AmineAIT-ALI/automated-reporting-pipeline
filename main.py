"""
main.py
-------
CLI entry-point for the Automated Reporting Pipeline.

Usage:
    # Run with defaults (data/raw → data/output)
    python main.py

    # Custom directories
    python main.py --input data/raw --output data/output

    # Generate synthetic data first, then run pipeline
    python main.py --generate-data

    # Verbose logging
    python main.py --verbose

    # Skip saving processed parquet files
    python main.py --no-save-processed
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to PYTHONPATH (handles execution from any working directory)
sys.path.insert(0, str(Path(__file__).parent))

from src.config import DB_PATH, OUTPUT_DIR, RAW_DIR, SQL_DIR
from src.pipeline import run_pipeline
from src.utils import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="automated-reporting-pipeline",
        description=(
            "Automated Reporting Pipeline — Extract, Transform, Load, "
            "compute KPIs, and generate business reports."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --input data/raw --output data/output
  python main.py --generate-data --verbose
        """,
    )

    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=RAW_DIR,
        metavar="DIR",
        help=f"Directory containing source CSV files (default: {RAW_DIR})",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=OUTPUT_DIR,
        metavar="DIR",
        help=f"Directory for generated reports (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        metavar="FILE",
        help=f"DuckDB database file path (default: {DB_PATH})",
    )
    parser.add_argument(
        "--generate-data",
        action="store_true",
        help="Generate synthetic CSV data before running the pipeline",
    )
    parser.add_argument(
        "--no-save-processed",
        action="store_true",
        help="Skip saving clean DataFrames to data/processed/ as Parquet",
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Suppress the KPI dashboard console output",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level logging",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level)

    # ── Optional: generate synthetic data ─────────────────────────────────────
    if args.generate_data:
        from scripts.generate_data import main as generate_main
        print("Generating synthetic datasets...")
        generate_main(args.input)

    # ── Run the pipeline ───────────────────────────────────────────────────────
    try:
        result = run_pipeline(
            raw_dir=args.input,
            output_dir=args.output,
            db_path=args.db,
            sql_dir=SQL_DIR,
            save_processed=not args.no_save_processed,
            print_summary=not args.no_summary,
        )
    except FileNotFoundError as exc:
        print(f"\n[ERROR] {exc}")
        print(
            "\nTip: Run with --generate-data to create synthetic CSV files first:\n"
            "  python main.py --generate-data\n"
        )
        return 1
    except Exception as exc:
        print(f"\n[ERROR] Pipeline failed: {exc}")
        raise  # re-raise for full traceback in verbose mode

    # ── Final summary ──────────────────────────────────────────────────────────
    print("Reports generated:")
    for name, path in result["outputs"].items():
        print(f"  [{name:8s}] {path.resolve()}")
    print(f"\nCompleted in {result['duration_s']:.2f}s  |  Run at: {result['run_at']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
