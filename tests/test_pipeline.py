"""
test_pipeline.py
----------------
Integration tests for the full pipeline execution (src/pipeline.py).

These tests use temporary directories with small synthetic datasets to
verify that the pipeline runs end-to-end without errors and produces
all expected output files.
"""

import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.pipeline import run_pipeline


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def temp_pipeline_dirs():
    """Create a temporary workspace with raw CSV files for end-to-end testing."""
    base = Path(tempfile.mkdtemp())
    raw_dir = base / "raw"
    output_dir = base / "output"
    processed_dir = base / "processed"
    raw_dir.mkdir()
    output_dir.mkdir()
    processed_dir.mkdir()

    # customers.csv
    pd.DataFrame({
        "customer_id": [f"CUST{i:03d}" for i in range(1, 21)],
        "customer_name": [f"Customer {i}" for i in range(1, 21)],
        "segment": (["Enterprise", "SMB", "Startup", "Consumer"] * 5),
        "city": ["Paris"] * 20,
        "country": ["France"] * 20,
        "signup_date": ["2022-01-01"] * 20,
    }).to_csv(raw_dir / "customers.csv", index=False)

    # products.csv
    pd.DataFrame({
        "product_id": [f"PROD{i:03d}" for i in range(1, 6)],
        "product_name": ["CRM Pro", "Server Rack", "Consulting", "Cloud S", "Security Suite"],
        "category": ["Software", "Hardware", "Services", "Cloud", "Security"],
        "unit_cost": [1200.0, 4500.0, 2000.0, 300.0, 800.0],
    }).to_csv(raw_dir / "products.csv", index=False)

    # orders.csv — 40 orders to ensure enough data for all KPIs
    import numpy as np
    rng = np.random.default_rng(42)
    n = 40
    statuses = rng.choice(
        ["completed", "cancelled", "returned", "pending"],
        size=n,
        p=[0.70, 0.12, 0.10, 0.08],
    )
    pd.DataFrame({
        "order_id": [f"ORD{i:04d}" for i in range(1, n + 1)],
        "customer_id": [f"CUST{rng.integers(1, 21):03d}" for _ in range(n)],
        "product_id": [f"PROD{rng.integers(1, 6):03d}" for _ in range(n)],
        "order_date": pd.date_range("2023-01-01", periods=n, freq="12D").strftime("%Y-%m-%d"),
        "quantity": rng.integers(1, 5, size=n),
        "unit_price": rng.uniform(500, 6000, size=n).round(2),
        "order_status": statuses,
    }).to_csv(raw_dir / "orders.csv", index=False)

    # deliveries.csv — for non-cancelled orders
    deliverable_ids = [f"ORD{i:04d}" for i in range(1, n + 1)
                       if statuses[i - 1] != "cancelled"]
    del_n = len(deliverable_ids)
    del_statuses = rng.choice(
        ["on_time", "late", "failed"],
        size=del_n,
        p=[0.78, 0.18, 0.04],
    )
    shipped = pd.date_range("2023-01-02", periods=del_n, freq="12D")
    delays = rng.integers(2, 10, size=del_n)
    delivered = [
        (shipped[i] + pd.Timedelta(days=int(delays[i]))).strftime("%Y-%m-%d")
        if del_statuses[i] != "failed" else None
        for i in range(del_n)
    ]
    pd.DataFrame({
        "delivery_id": [f"DEL{i:05d}" for i in range(1, del_n + 1)],
        "order_id": deliverable_ids,
        "carrier": rng.choice(["DHL", "FedEx", "UPS"], size=del_n),
        "shipped_date": shipped.strftime("%Y-%m-%d"),
        "delivered_date": delivered,
        "delivery_status": del_statuses,
    }).to_csv(raw_dir / "deliveries.csv", index=False)

    yield {
        "base": base,
        "raw_dir": raw_dir,
        "output_dir": output_dir,
        "db_path": base / "pipeline.duckdb",
        "sql_dir": Path(__file__).parent.parent / "sql",
        "processed_dir": processed_dir,
    }

    shutil.rmtree(base, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# Full pipeline integration tests
# ─────────────────────────────────────────────────────────────────────────────

class TestRunPipeline:
    def test_pipeline_completes_without_error(self, temp_pipeline_dirs):
        result = run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        assert result is not None

    def test_pipeline_returns_expected_keys(self, temp_pipeline_dirs):
        result = run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        for key in ["tables", "kpis", "outputs", "duration_s", "run_at"]:
            assert key in result

    def test_output_csv_created(self, temp_pipeline_dirs):
        run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        assert (temp_pipeline_dirs["output_dir"] / "kpi_summary.csv").exists()

    def test_output_excel_created(self, temp_pipeline_dirs):
        run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        assert (temp_pipeline_dirs["output_dir"] / "kpi_summary.xlsx").exists()

    def test_output_markdown_created(self, temp_pipeline_dirs):
        run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        assert (temp_pipeline_dirs["output_dir"] / "report.md").exists()

    def test_kpis_contain_required_keys(self, temp_pipeline_dirs):
        result = run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        required_kpi_keys = [
            "total_orders", "total_revenue", "average_order_value",
            "total_customers", "on_time_delivery_rate", "cancellation_rate",
            "top_products", "monthly_revenue", "top_segments",
        ]
        for key in required_kpi_keys:
            assert key in result["kpis"], f"Missing KPI: {key}"

    def test_total_orders_positive(self, temp_pipeline_dirs):
        result = run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        assert result["kpis"]["total_orders"] > 0

    def test_total_revenue_positive(self, temp_pipeline_dirs):
        result = run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        assert result["kpis"]["total_revenue"] > 0

    def test_duration_is_recorded(self, temp_pipeline_dirs):
        result = run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        assert result["duration_s"] > 0

    def test_markdown_report_contains_title(self, temp_pipeline_dirs):
        run_pipeline(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        md_content = (temp_pipeline_dirs["output_dir"] / "report.md").read_text()
        assert "Automated Business Performance Report" in md_content
        assert "KPI Summary" in md_content

    def test_idempotency(self, temp_pipeline_dirs):
        """Running the pipeline twice should produce the same KPIs."""
        kwargs = dict(
            raw_dir=temp_pipeline_dirs["raw_dir"],
            output_dir=temp_pipeline_dirs["output_dir"],
            db_path=temp_pipeline_dirs["db_path"],
            sql_dir=temp_pipeline_dirs["sql_dir"],
            print_summary=False,
        )
        result1 = run_pipeline(**kwargs)
        result2 = run_pipeline(**kwargs)
        assert result1["kpis"]["total_orders"] == result2["kpis"]["total_orders"]
        assert result1["kpis"]["total_revenue"] == result2["kpis"]["total_revenue"]
