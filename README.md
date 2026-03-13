# Automated Reporting Pipeline

> **Automated business performance reporting — from raw CSV data to actionable KPI reports in a single command.**

---

## Overview

The **Automated Reporting Pipeline** is a production-grade Python + SQL data pipeline that automates the full reporting cycle for business performance analysis:

- **Extract** raw commercial and operational data from CSV sources
- **Transform** and clean the data (deduplication, type casting, anomaly handling)
- **Load** clean tables into a local analytical database (DuckDB)
- **Compute KPIs** using SQL and pandas (dual-engine cross-validation)
- **Generate reports** in CSV, Excel, and Markdown — ready for dashboards, email distribution, and Git versioning

Built as a professional portfolio project demonstrating skills in **Data Engineering**, **Analytics Engineering**, **Python**, and **SQL**.

---

## Business Context

This pipeline simulates a commercial operations reporting workflow for a B2B software/services company. The data covers orders, customers, products, and delivery logistics — the typical sources for a weekly or monthly performance review.

**Problem solved:** manual reporting from multiple data sources is slow, error-prone, and inconsistent. This pipeline makes reporting reproducible, automated, and auditable.

---

## Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Data manipulation | pandas 2.x, numpy |
| Analytical database | DuckDB (embedded, file-based) |
| Excel export | openpyxl |
| Processed layer | Apache Parquet (via pyarrow) |
| Testing | pytest + pytest-cov |
| CLI | argparse (stdlib) |
| Logging | logging (stdlib) |

No cloud services, no Docker, no heavy frameworks — runs fully locally.

---

## Architecture

```
Raw CSVs
   │
   ▼
┌─────────────────────────────────────────────────────────────┐
│  EXTRACT   Load & validate schema contracts                 │
│  src/extract.py                                             │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORM  Dedup, cast types, clean anomalies, join        │
│  src/transform.py        →  data/processed/*.parquet        │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  LOAD       Persist clean tables to DuckDB                  │
│  src/load.py             →  data/pipeline.duckdb            │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  METRICS    Compute KPIs (SQL engine + pandas validation)   │
│  src/metrics.py                                             │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  REPORT     Generate CSV / Excel / Markdown outputs         │
│  src/report.py           →  data/output/                    │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
automated-reporting-pipeline/
│
├── data/
│   ├── raw/                    # Source CSV files (input)
│   │   ├── orders.csv
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   └── deliveries.csv
│   ├── processed/              # Clean Parquet files (intermediate)
│   └── output/                 # Generated reports (final)
│       ├── kpi_summary.csv
│       ├── kpi_summary.xlsx
│       ├── report.md
│       ├── top_products.csv
│       ├── monthly_revenue.csv
│       ├── top_segments.csv
│       ├── revenue_by_category.csv
│       └── revenue_by_country.csv
│
├── sql/
│   ├── schema.sql              # DuckDB table definitions
│   └── kpi_queries.sql         # Reference SQL KPI queries (standalone)
│
├── src/
│   ├── __init__.py
│   ├── config.py               # Paths, constants, schema contracts
│   ├── extract.py              # CSV loading and schema validation
│   ├── transform.py            # Cleaning, normalisation, fact table
│   ├── load.py                 # DuckDB persistence layer
│   ├── metrics.py              # KPI computation (SQL + pandas)
│   ├── report.py               # CSV / Excel / Markdown generation
│   ├── pipeline.py             # Full pipeline orchestration
│   └── utils.py                # Logging, formatters, helpers
│
├── scripts/
│   └── generate_data.py        # Synthetic dataset generator
│
├── tests/
│   ├── conftest.py             # Shared pytest fixtures
│   ├── test_transform.py       # Unit tests for transform layer
│   ├── test_metrics.py         # Unit + cross-validation tests for KPIs
│   └── test_pipeline.py        # End-to-end integration tests
│
├── main.py                     # CLI entry-point
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Installation

**Requirements:** Python 3.11+

```bash
# 1. Clone the repository
git clone https://github.com/AmineAIT-ALI/automated-reporting-pipeline.git
cd automated-reporting-pipeline

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Quick Start

### Step 1 — Generate synthetic data

```bash
python scripts/generate_data.py
```

This creates ~800 realistic orders, 150 customers, 40 products, and ~680 deliveries in `data/raw/`.

### Step 2 — Run the pipeline

```bash
python main.py
```

Or in a single command:

```bash
python main.py --generate-data
```

### Step 3 — Check the outputs

```
data/output/
├── kpi_summary.csv       ← flat KPI table (for BI ingestion)
├── kpi_summary.xlsx      ← multi-sheet Excel workbook (for stakeholders)
├── report.md             ← Markdown analytical report (for Git / docs)
├── top_products.csv
├── monthly_revenue.csv
├── top_segments.csv
├── revenue_by_category.csv
└── revenue_by_country.csv
```

---

## CLI Reference

```
usage: automated-reporting-pipeline [-h] [--input DIR] [--output DIR]
                                     [--db FILE] [--generate-data]
                                     [--no-save-processed] [--no-summary]
                                     [--verbose]

Options:
  --input  DIR      Source CSV directory        (default: data/raw)
  --output DIR      Output reports directory    (default: data/output)
  --db     FILE     DuckDB database path        (default: data/pipeline.duckdb)
  --generate-data   Generate synthetic data before running
  --no-save-processed  Skip saving Parquet files to data/processed/
  --no-summary      Suppress console KPI dashboard
  --verbose, -v     Enable DEBUG logging
```

---

## KPI Catalogue

| KPI | Description | Engine |
|---|---|---|
| `total_orders` | Total number of orders (all statuses) | SQL + pandas |
| `total_revenue` | Sum of revenue from completed orders | SQL + pandas |
| `average_order_value` | Average revenue per completed order | SQL + pandas |
| `total_customers` | Count of distinct customers | SQL + pandas |
| `on_time_delivery_rate` | % of deliveries arriving on time | SQL + pandas |
| `late_delivery_rate` | % of deliveries arriving late | SQL + pandas |
| `average_delivery_delay_days` | Mean days from ship to delivery | SQL + pandas |
| `cancellation_rate` | % of cancelled orders | SQL + pandas |
| `return_rate` | % of returned orders | SQL + pandas |
| `top_5_products_by_revenue` | Products ranked by completed revenue | SQL + pandas |
| `monthly_revenue_trend` | Revenue and order count per month | SQL + pandas |
| `top_customer_segments` | Segments ranked by revenue + orders | SQL + pandas |
| `revenue_by_category` | Revenue breakdown by product category | SQL |
| `revenue_by_country` | Revenue breakdown by customer country | SQL |

All KPIs are computed via **two independent engines** (SQL and pandas) and cross-validated at runtime. Any divergence above 1% is logged as a warning.

---

## Data Sources

| File | Rows | Description |
|---|---|---|
| `orders.csv` | ~804 | Order lines with status, quantity, price |
| `customers.csv` | ~152 | Customer master with segment and geography |
| `products.csv` | 39 | Product catalogue with cost |
| `deliveries.csv` | ~580 | Delivery events with carrier and dates |

**Injected anomalies** (for cleaning demonstration):
- Duplicate rows on primary keys
- Negative quantities
- Missing unit prices (imputed with per-product median)
- Unparseable dates (dropped)
- Invalid order/delivery status values (dropped)

---

## Running Tests

```bash
# Run all tests
pytest

# With coverage report
pytest --cov=src --cov-report=term-missing

# Run a single test file
pytest tests/test_transform.py -v
```

Test coverage targets:
- `test_transform.py` — 20+ unit tests for all cleaning functions
- `test_metrics.py` — KPI correctness, edge cases, SQL↔pandas cross-validation
- `test_pipeline.py` — end-to-end integration including idempotency check

---

## Console Output Example

```
════════════════════════════════════════════════════════════
  BUSINESS PERFORMANCE KPI DASHBOARD
════════════════════════════════════════════════════════════

  COMMERCIAL METRICS
  ────────────────────────────────────────────────────────
  Total Orders                          756
  Total Revenue                   €4,312,847.50
  Average Order Value               €6,537.24
  Total Customers                       150
  Cancellation Rate                   10.0%
  Return Rate                          9.9%

  DELIVERY METRICS
  ────────────────────────────────────────────────────────
  On-Time Delivery Rate               78.0%
  Late Delivery Rate                  18.0%
  Avg Delivery Delay (days)            4.2d

  TOP 5 PRODUCTS BY REVENUE
  ────────────────────────────────────────────────────────
  Data Warehouse                  €482,100.00
  ML Platform                     €437,850.00
  ERP Suite                       €398,200.00
  GPU Compute Node                €362,400.00
  SIEM License                    €287,300.00

  TOP CUSTOMER SEGMENTS
  ────────────────────────────────────────────────────────
  SMB                           €1,287,350.00
  Enterprise                    €1,043,200.00
  Startup                         €892,100.00
════════════════════════════════════════════════════════════
```

---

## Data Flow

```
orders.csv ──┐
              ├─── clean_orders()    ──┐
customers.csv ├─── clean_customers() ──┤
              ├─── clean_products()  ──┼─── build_fact_table() ─── DuckDB ─── KPIs ─── Reports
products.csv ─┤                        │
              └─── clean_deliveries() ─┘
deliveries.csv
```

---

## Business Impact

> "This pipeline eliminates manual Excel consolidation work by automating the full reporting chain — from raw data ingestion to formatted stakeholder deliverables — in under 10 seconds."

- **Reproducible:** any team member can run `python main.py` and get identical results
- **Auditable:** all cleaning decisions are logged; processed Parquet files preserve the clean state
- **Extensible:** adding a new KPI requires modifying only `src/metrics.py` and `sql/kpi_queries.sql`
- **Testable:** 30+ unit and integration tests prevent regressions

---

## Potential Improvements

| Area | Idea |
|---|---|
| Scheduling | Add a cron job or Airflow DAG for daily/weekly runs |
| Alerting | Send email or Slack notification when KPIs cross thresholds |
| Visualisation | Add a Streamlit dashboard reading from DuckDB |
| Incremental load | Replace full reload with delta/upsert strategy |
| Cloud storage | Read CSVs from S3 / GCS instead of local filesystem |
| Data quality | Integrate Great Expectations for richer validation |
| CI/CD | GitHub Actions workflow to run tests on every push |

---

## Author

**Amine AIT ALI** — [github.com/AmineAIT-ALI](https://github.com/AmineAIT-ALI)

Built as a portfolio project demonstrating Data Engineering and Analytics Engineering competencies.

**Stack:** Python · SQL · DuckDB · pandas · pytest · openpyxl

---

*"Automatisation du reporting pour un suivi décisionnel rapide, fiable et reproductible."*
