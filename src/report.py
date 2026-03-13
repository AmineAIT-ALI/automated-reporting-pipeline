"""
report.py
---------
Reporting layer: generate CSV, Excel, and Markdown report outputs.

Outputs written to data/output/:
  - kpi_summary.csv    — flat KPI table for BI tools / downstream pipelines
  - kpi_summary.xlsx   — multi-sheet Excel workbook for stakeholder distribution
  - report.md          — human-readable analytical report for Git / documentation
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.config import (
    KPI_CSV_NAME, KPI_EXCEL_NAME, REPORT_MD_NAME,
    OUTPUT_DIR, REPORT_TITLE, TOP_N_PRODUCTS,
)
from src.utils import (
    ensure_directories, fmt_currency, fmt_number, fmt_percent,
    get_logger, md_table, now_str, now_filename_str,
)

logger = get_logger("report")

KPIResult = dict[str, Any]


# ── CSV export ─────────────────────────────────────────────────────────────────

def generate_csv(kpis: KPIResult, output_dir: Path = OUTPUT_DIR) -> Path:
    """
    Export scalar KPIs as a flat CSV (key–value format) plus one CSV per
    tabular KPI.
    """
    ensure_directories(output_dir)
    scalar_kpis = {k: v for k, v in kpis.items() if not isinstance(v, pd.DataFrame)}
    path = output_dir / KPI_CSV_NAME

    rows = [{"kpi": k, "value": v} for k, v in scalar_kpis.items()]
    pd.DataFrame(rows).to_csv(path, index=False)
    logger.info(f"CSV exported: {path}")

    # Export tabular KPIs as separate CSVs
    tabular_names = {
        "top_products": "top_products.csv",
        "monthly_revenue": "monthly_revenue.csv",
        "top_segments": "top_segments.csv",
        "revenue_by_category": "revenue_by_category.csv",
        "revenue_by_country": "revenue_by_country.csv",
    }
    for key, filename in tabular_names.items():
        if key in kpis and isinstance(kpis[key], pd.DataFrame):
            tab_path = output_dir / filename
            kpis[key].to_csv(tab_path, index=False)
            logger.info(f"  Tabular CSV exported: {tab_path}")

    return path


# ── Excel export ───────────────────────────────────────────────────────────────

def generate_excel(kpis: KPIResult, output_dir: Path = OUTPUT_DIR) -> Path:
    """
    Export a multi-sheet Excel workbook:
      Sheet 1 — KPI Summary (scalar KPIs)
      Sheet 2 — Top Products
      Sheet 3 — Monthly Revenue Trend
      Sheet 4 — Customer Segments
      Sheet 5 — Revenue by Category
      Sheet 6 — Revenue by Country (if available)
    """
    ensure_directories(output_dir)
    path = output_dir / KPI_EXCEL_NAME

    scalar_kpis = {k: v for k, v in kpis.items() if not isinstance(v, pd.DataFrame)}
    kpi_df = pd.DataFrame([
        {"KPI": k.replace("_", " ").title(), "Value": v}
        for k, v in scalar_kpis.items()
    ])

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        # ── Sheet: KPI Summary ────────────────────────────────────────────────
        kpi_df.to_excel(writer, sheet_name="KPI Summary", index=False)
        _style_sheet(writer, "KPI Summary", kpi_df)

        # ── Sheet: Top Products ───────────────────────────────────────────────
        if "top_products" in kpis and isinstance(kpis["top_products"], pd.DataFrame):
            kpis["top_products"].to_excel(writer, sheet_name="Top Products", index=False)
            _style_sheet(writer, "Top Products", kpis["top_products"])

        # ── Sheet: Monthly Revenue ────────────────────────────────────────────
        if "monthly_revenue" in kpis and isinstance(kpis["monthly_revenue"], pd.DataFrame):
            kpis["monthly_revenue"].to_excel(
                writer, sheet_name="Monthly Revenue", index=False
            )
            _style_sheet(writer, "Monthly Revenue", kpis["monthly_revenue"])

        # ── Sheet: Customer Segments ──────────────────────────────────────────
        if "top_segments" in kpis and isinstance(kpis["top_segments"], pd.DataFrame):
            kpis["top_segments"].to_excel(
                writer, sheet_name="Customer Segments", index=False
            )
            _style_sheet(writer, "Customer Segments", kpis["top_segments"])

        # ── Sheet: Revenue by Category ────────────────────────────────────────
        if "revenue_by_category" in kpis and isinstance(kpis["revenue_by_category"], pd.DataFrame):
            kpis["revenue_by_category"].to_excel(
                writer, sheet_name="Revenue by Category", index=False
            )
            _style_sheet(writer, "Revenue by Category", kpis["revenue_by_category"])

        # ── Sheet: Revenue by Country ─────────────────────────────────────────
        if "revenue_by_country" in kpis and isinstance(kpis["revenue_by_country"], pd.DataFrame):
            kpis["revenue_by_country"].to_excel(
                writer, sheet_name="Revenue by Country", index=False
            )
            _style_sheet(writer, "Revenue by Country", kpis["revenue_by_country"])

    logger.info(f"Excel workbook exported: {path}")
    return path


def _style_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    df: pd.DataFrame,
) -> None:
    """Auto-fit column widths for readability."""
    worksheet = writer.sheets[sheet_name]
    for col_idx, column in enumerate(df.columns, start=1):
        max_len = max(len(str(column)), df[column].astype(str).map(len).max())
        worksheet.column_dimensions[
            worksheet.cell(row=1, column=col_idx).column_letter
        ].width = min(max_len + 4, 50)


# ── Markdown report ────────────────────────────────────────────────────────────

def generate_markdown(
    kpis: KPIResult,
    output_dir: Path = OUTPUT_DIR,
    generated_at: str | None = None,
) -> Path:
    """
    Generate a structured Markdown analytical report with:
    - Executive summary
    - KPI table
    - Key insights
    - Points of attention
    """
    ensure_directories(output_dir)
    path = output_dir / REPORT_MD_NAME
    ts = generated_at or now_str()

    on_time_pct = fmt_percent(kpis.get("on_time_delivery_rate", 0))
    cancel_pct = fmt_percent(kpis.get("cancellation_rate", 0))
    return_pct = fmt_percent(kpis.get("return_rate", 0))
    late_pct = fmt_percent(kpis.get("late_delivery_rate", 0))
    avg_delay = kpis.get("average_delivery_delay_days", 0)

    # Determine trend (last 2 months)
    monthly = kpis.get("monthly_revenue")
    trend_comment = ""
    if isinstance(monthly, pd.DataFrame) and len(monthly) >= 2:
        last = monthly.iloc[-1]["total_revenue"]
        prev = monthly.iloc[-2]["total_revenue"]
        delta_pct = ((last - prev) / prev * 100) if prev > 0 else 0
        arrow = "▲" if delta_pct >= 0 else "▼"
        trend_comment = (
            f"Month-over-month revenue trend: {arrow} {abs(delta_pct):.1f}% "
            f"({monthly.iloc[-2]['year_month']} → {monthly.iloc[-1]['year_month']})"
        )

    # Scalar KPI table
    scalar_rows = [
        ["Total Orders", fmt_number(kpis.get("total_orders", 0))],
        ["Total Revenue", fmt_currency(kpis.get("total_revenue", 0))],
        ["Average Order Value", fmt_currency(kpis.get("average_order_value", 0))],
        ["Total Customers", fmt_number(kpis.get("total_customers", 0))],
        ["On-Time Delivery Rate", on_time_pct],
        ["Late Delivery Rate", late_pct],
        ["Avg Delivery Delay (days)", f"{avg_delay:.1f}"],
        ["Cancellation Rate", cancel_pct],
        ["Return Rate", return_pct],
    ]
    kpi_table = md_table(["KPI", "Value"], scalar_rows)

    # Top products table
    top_products_md = ""
    if isinstance(kpis.get("top_products"), pd.DataFrame):
        tp = kpis["top_products"]
        headers = ["Rank", "Product", "Revenue", "Units Sold"]
        rows = [
            [
                str(i + 1),
                str(r.get("product_name", "")),
                fmt_currency(float(r.get("total_revenue", 0))),
                fmt_number(int(r.get("total_units_sold", 0))),
            ]
            for i, (_, r) in enumerate(tp.iterrows())
        ]
        top_products_md = md_table(headers, rows)

    # Monthly revenue table
    monthly_md = ""
    if isinstance(kpis.get("monthly_revenue"), pd.DataFrame):
        mr = kpis["monthly_revenue"]
        headers = ["Month", "Revenue", "Orders"]
        rows = [
            [
                str(r["year_month"]),
                fmt_currency(float(r["total_revenue"])),
                fmt_number(int(r.get("order_count", 0))),
            ]
            for _, r in mr.iterrows()
        ]
        monthly_md = md_table(headers, rows)

    # Segment table
    segments_md = ""
    if isinstance(kpis.get("top_segments"), pd.DataFrame):
        ts_df = kpis["top_segments"]
        headers = ["Segment", "Revenue", "Orders", "Customers"]
        rows = [
            [
                str(r["segment"]),
                fmt_currency(float(r["total_revenue"])),
                fmt_number(int(r.get("order_count", 0))),
                fmt_number(int(r.get("unique_customers", 0))),
            ]
            for _, r in ts_df.iterrows()
        ]
        segments_md = md_table(headers, rows)

    # ── Insights ───────────────────────────────────────────────────────────────
    insights = []
    if kpis.get("on_time_delivery_rate", 0) >= 0.80:
        insights.append(
            f"On-time delivery rate is strong at {on_time_pct}, indicating reliable logistics operations."
        )
    if kpis.get("cancellation_rate", 0) < 0.10:
        insights.append(
            f"Cancellation rate ({cancel_pct}) is below the 10% threshold — customer intent is high."
        )
    if trend_comment:
        insights.append(trend_comment)

    # ── Attention points ───────────────────────────────────────────────────────
    alerts = []
    if kpis.get("late_delivery_rate", 0) > 0.15:
        alerts.append(
            f"Late delivery rate ({late_pct}) exceeds 15% — carrier performance should be reviewed."
        )
    if kpis.get("cancellation_rate", 0) > 0.12:
        alerts.append(
            f"Cancellation rate ({cancel_pct}) is elevated — investigate pricing or UX friction."
        )
    if kpis.get("average_delivery_delay_days", 0) > 7:
        alerts.append(
            f"Average delivery delay ({avg_delay:.1f} days) exceeds 7 days — consider SLA review."
        )
    if not alerts:
        alerts.append("No critical alerts — all KPIs are within acceptable ranges.")

    def _bullet_list(items: list[str]) -> str:
        return "\n".join(f"- {item}" for item in items)

    md = f"""# {REPORT_TITLE}

> **Generated:** {ts}

---

## Executive Summary

This report provides an automated snapshot of business performance across commercial,
operational, and logistics dimensions. It is generated by the Automated Reporting Pipeline
and refreshed on each pipeline run.

**Period covered:** Based on available order data in the pipeline database.

---

## KPI Summary

{kpi_table}

---

## Top {TOP_N_PRODUCTS} Products by Revenue

{top_products_md}

---

## Monthly Revenue Trend

{monthly_md}

---

## Customer Segments

{segments_md}

---

## Key Insights

{_bullet_list(insights) if insights else "_No notable insights detected._"}

---

## Points of Attention

{_bullet_list(alerts)}

---

## Notes

- Revenue figures include only **completed** orders.
- Delivery metrics exclude cancelled orders (no delivery attempt).
- All monetary values are in **EUR**.
- This report was automatically generated — no manual intervention required.

---

*Automated Reporting Pipeline — [github.com/AmineAIT-ALI/automated-reporting-pipeline](https://github.com/AmineAIT-ALI/automated-reporting-pipeline)*
"""

    path.write_text(md, encoding="utf-8")
    logger.info(f"Markdown report exported: {path}")
    return path


# ── Convenience: generate all outputs ─────────────────────────────────────────

def generate_all(kpis: KPIResult, output_dir: Path = OUTPUT_DIR) -> dict[str, Path]:
    """Generate CSV, Excel, and Markdown outputs. Returns paths dict."""
    logger.info("=== REPORT step started ===")
    paths = {
        "csv": generate_csv(kpis, output_dir),
        "excel": generate_excel(kpis, output_dir),
        "markdown": generate_markdown(kpis, output_dir),
    }
    logger.info(
        "=== REPORT step completed — "
        + ", ".join(f"{k}: {p.name}" for k, p in paths.items())
        + " ==="
    )
    return paths
