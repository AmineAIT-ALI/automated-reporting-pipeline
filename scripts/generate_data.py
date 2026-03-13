"""
generate_data.py
----------------
Generates realistic synthetic CSV datasets for the Automated Reporting Pipeline.

Datasets produced:
  - customers.csv  (~150 customers)
  - products.csv   (~40 products across 6 categories)
  - orders.csv     (~800 orders over 18 months)
  - deliveries.csv (~680 deliveries, excluding cancelled orders)

Usage:
    python scripts/generate_data.py
    python scripts/generate_data.py --output data/raw
"""

import argparse
import random
import string
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ── Reproducibility ────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# ── Configuration ──────────────────────────────────────────────────────────────
N_CUSTOMERS = 150
N_PRODUCTS = 40
N_ORDERS = 800
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 6, 30)

SEGMENTS = ["Enterprise", "SMB", "Startup", "Consumer", "Government"]
CITIES = [
    ("Paris", "France"), ("Lyon", "France"), ("Marseille", "France"),
    ("Bordeaux", "France"), ("Toulouse", "France"), ("Nantes", "France"),
    ("Strasbourg", "France"), ("Lille", "France"), ("Nice", "France"),
    ("Rennes", "France"), ("Bruxelles", "Belgique"), ("Genève", "Suisse"),
    ("Luxembourg", "Luxembourg"), ("Montreal", "Canada"),
]
CATEGORIES = {
    "Software": ["CRM Pro", "ERP Suite", "Analytics Dashboard", "Security Suite",
                 "Collaboration Hub", "DevOps Platform", "DataViz Tool", "API Gateway"],
    "Hardware": ["Server Rack 1U", "NAS Storage 8TB", "Network Switch 24P",
                 "GPU Compute Node", "Edge Router", "Wireless AP"],
    "Services": ["Consulting Pack", "Training Session", "Support Contract",
                 "Audit Service", "Integration Service", "Migration Pack"],
    "Cloud": ["Cloud Storage 1TB", "Cloud Compute S", "Cloud Compute M",
              "Cloud Compute L", "CDN Package", "Backup Service"],
    "Data": ["Data Warehouse", "ETL Pipeline", "ML Platform", "Data Lake",
             "Streaming Engine"],
    "Security": ["Firewall License", "SIEM License", "Endpoint Protection",
                 "VPN Gateway", "Identity Manager"],
}
CARRIERS = ["DHL", "FedEx", "UPS", "Chronopost", "TNT", "DB Schenker"]
ORDER_STATUSES = ["completed", "cancelled", "pending", "returned"]
ORDER_STATUS_WEIGHTS = [0.72, 0.10, 0.08, 0.10]
DELIVERY_STATUSES = ["on_time", "late", "failed"]
DELIVERY_STATUS_WEIGHTS = [0.78, 0.18, 0.04]


# ── Helpers ────────────────────────────────────────────────────────────────────

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def random_id(prefix: str, n: int, total: int) -> list[str]:
    width = len(str(total))
    return [f"{prefix}{str(i).zfill(width)}" for i in range(1, n + 1)]


# ── Generators ─────────────────────────────────────────────────────────────────

def generate_customers(n: int) -> pd.DataFrame:
    first_names = ["Alice", "Bob", "Charlotte", "David", "Emma", "François",
                   "Gabriel", "Hélène", "Isabelle", "Julien", "Karim", "Laura",
                   "Mathieu", "Nina", "Olivier", "Pauline", "Quentin", "Rachel",
                   "Sébastien", "Théo", "Ugo", "Valérie", "William", "Xavier",
                   "Yasmine", "Zoé"]
    last_names = ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard",
                  "Petit", "Durand", "Leroy", "Moreau", "Simon", "Laurent",
                  "Lefebvre", "Michel", "Garcia", "David", "Bertrand", "Roux",
                  "Vincent", "Fournier", "Morel", "Girard", "André", "Lefevre"]

    customer_ids = random_id("CUST", n, n)
    names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(n)]
    segments = random.choices(SEGMENTS, weights=[0.15, 0.30, 0.25, 0.20, 0.10], k=n)
    city_country = [random.choice(CITIES) for _ in range(n)]
    cities = [c[0] for c in city_country]
    countries = [c[1] for c in city_country]
    signup_dates = [random_date(datetime(2020, 1, 1), START_DATE).strftime("%Y-%m-%d")
                    for _ in range(n)]

    # Inject 2 duplicate rows for testing dedup
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "customer_name": names,
        "segment": segments,
        "city": cities,
        "country": countries,
        "signup_date": signup_dates,
    })
    duplicates = df.sample(2, random_state=SEED)
    df = pd.concat([df, duplicates], ignore_index=True)
    return df


def generate_products(categories: dict) -> pd.DataFrame:
    rows = []
    pid = 1
    for category, product_names in categories.items():
        for name in product_names:
            base_cost = {
                "Software": np.random.uniform(200, 2000),
                "Hardware": np.random.uniform(500, 8000),
                "Services": np.random.uniform(800, 5000),
                "Cloud": np.random.uniform(50, 1500),
                "Data": np.random.uniform(1000, 10000),
                "Security": np.random.uniform(300, 3000),
            }[category]
            rows.append({
                "product_id": f"PROD{str(pid).zfill(3)}",
                "product_name": name,
                "category": category,
                "unit_cost": round(base_cost, 2),
            })
            pid += 1
    df = pd.DataFrame(rows)
    return df


def generate_orders(customers: pd.DataFrame, products: pd.DataFrame, n: int) -> pd.DataFrame:
    # Weight towards more recent dates (sales growth simulation)
    total_seconds = int((END_DATE - START_DATE).total_seconds())
    timestamps = np.sort(
        np.random.choice(total_seconds, size=n, replace=False, p=None)
    )
    order_dates = [START_DATE + timedelta(seconds=int(s)) for s in timestamps]

    customer_ids = random.choices(
        customers["customer_id"].tolist(), k=n
    )
    product_ids = random.choices(
        products["product_id"].tolist(), k=n
    )
    quantities = np.random.choice([1, 2, 3, 5, 10], size=n, p=[0.40, 0.30, 0.15, 0.10, 0.05])

    # Unit price = cost * margin multiplier
    price_map = products.set_index("product_id")["unit_cost"].to_dict()
    unit_prices = [
        round(price_map[pid] * np.random.uniform(1.20, 1.60), 2)
        for pid in product_ids
    ]
    statuses = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=n)

    df = pd.DataFrame({
        "order_id": random_id("ORD", n, n),
        "customer_id": customer_ids,
        "product_id": product_ids,
        "order_date": [d.strftime("%Y-%m-%d") for d in order_dates],
        "quantity": quantities,
        "unit_price": unit_prices,
        "order_status": statuses,
    })

    # Inject controlled anomalies
    # 5 orders with null unit_price
    null_idx = df.sample(5, random_state=SEED).index
    df.loc[null_idx, "unit_price"] = None
    # 3 orders with negative quantity (data quality issue)
    neg_idx = df.sample(3, random_state=SEED + 1).index
    df.loc[neg_idx, "quantity"] = -1
    # 4 duplicate rows
    duplicates = df.sample(4, random_state=SEED + 2)
    df = pd.concat([df, duplicates], ignore_index=True)

    return df


def generate_deliveries(orders: pd.DataFrame) -> pd.DataFrame:
    # Only non-cancelled orders get a delivery
    deliverable = orders[~orders["order_date"].isna() &
                         orders["order_status"].isin(["completed", "returned", "pending"])].copy()

    # Remove injected duplicates for delivery generation
    deliverable = deliverable.drop_duplicates(subset="order_id")

    rows = []
    for i, (_, order) in enumerate(deliverable.iterrows()):
        order_dt = datetime.strptime(str(order["order_date"]), "%Y-%m-%d")
        ship_delay = timedelta(days=random.randint(1, 3))
        shipped_dt = order_dt + ship_delay

        status = random.choices(DELIVERY_STATUSES, weights=DELIVERY_STATUS_WEIGHTS, k=1)[0]

        if status == "on_time":
            delivery_days = random.randint(2, 5)
        elif status == "late":
            delivery_days = random.randint(6, 15)
        else:  # failed
            delivery_days = None

        delivered_dt = (shipped_dt + timedelta(days=delivery_days)).strftime("%Y-%m-%d") \
            if delivery_days else None

        rows.append({
            "delivery_id": f"DEL{str(i + 1).zfill(5)}",
            "order_id": order["order_id"],
            "carrier": random.choice(CARRIERS),
            "shipped_date": shipped_dt.strftime("%Y-%m-%d"),
            "delivered_date": delivered_dt,
            "delivery_status": status,
        })

    df = pd.DataFrame(rows)

    # Inject 3 duplicate deliveries
    duplicates = df.sample(3, random_state=SEED)
    df = pd.concat([df, duplicates], ignore_index=True)

    return df


# ── Main ───────────────────────────────────────────────────────────────────────

def main(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Generating customers...")
    customers = generate_customers(N_CUSTOMERS)
    customers.to_csv(output_dir / "customers.csv", index=False)
    print(f"  → {len(customers)} rows written (incl. duplicates)")

    print("Generating products...")
    products = generate_products(CATEGORIES)
    products.to_csv(output_dir / "products.csv", index=False)
    print(f"  → {len(products)} rows written")

    print("Generating orders...")
    orders = generate_orders(customers.drop_duplicates("customer_id"), products, N_ORDERS)
    orders.to_csv(output_dir / "orders.csv", index=False)
    print(f"  → {len(orders)} rows written (incl. duplicates + anomalies)")

    print("Generating deliveries...")
    deliveries = generate_deliveries(orders.drop_duplicates("order_id"))
    deliveries.to_csv(output_dir / "deliveries.csv", index=False)
    print(f"  → {len(deliveries)} rows written (incl. duplicates)")

    print(f"\nAll datasets written to: {output_dir.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic pipeline datasets.")
    parser.add_argument(
        "--output", type=Path,
        default=Path(__file__).parent.parent / "data" / "raw",
        help="Output directory for CSV files (default: data/raw)"
    )
    args = parser.parse_args()
    main(args.output)
