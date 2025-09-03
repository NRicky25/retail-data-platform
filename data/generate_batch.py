import argparse
import os
from pathlib import Path
from datetime import datetime, timedelta
import random
import numpy as np
import pandas as pd
from faker import Faker

REGIONS = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT"]
CATEGORIES = [
    "Electronics", "Home", "Groceries", "Clothing",
    "Sports", "Beauty", "Toys", "Automotive"
]

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def seeded_faker(seed: int):
    fake = Faker("en_AU")
    Faker.seed(seed)
    random.seed(seed)
    np.random.seed(seed)
    return fake

CATEGORY_PRICE_RANGES = {
    "Electronics": (40, 2000),
    "Home": (10, 600),
    "Groceries": (2, 80),
    "Clothing": (8, 300),
    "Sports": (10, 800),
    "Beauty": (5, 200),
    "Toys": (5, 150),
    "Automotive": (15, 900),
}

def sample_price(category: str) -> float:
    low, high = CATEGORY_PRICE_RANGES.get(category, (10, 500))
    # log-normal-ish: bias toward lower prices with occasional higher spend
    val = np.random.lognormal(mean=2.8, sigma=0.6)  # ~16 -> 1k
    # scale into [low, high]
    val = low + (min(val, 10_000) / 10_000) * (high - low)
    return round(float(val), 2)

def generate_customers(n_customers: int, fake: Faker, start_id: int = 1) -> pd.DataFrame:
    rows = []
    today = datetime.utcnow().date()
    for cid in range(start_id, start_id + n_customers):
        name = fake.name()
        email = fake.unique.safe_email()
        region = random.choice(REGIONS)
        join_date = today - timedelta(days=random.randint(30, 3 * 365))
        rows.append(
            {
                "customer_id": cid,
                "name": name,
                "email": email,
                "region": region,
                "join_date": join_date.isoformat(),
            }
        )
    return pd.DataFrame(rows)

def generate_products(n_products: int, fake: Faker, start_id: int = 1000) -> pd.DataFrame:
    rows = []
    for pid in range(start_id, start_id + n_products):
        category = random.choice(CATEGORIES)
        # Generate a plausible product name
        product_name = f"{fake.color_name()} {category[:-1] if category.endswith('s') else category}"
        price = sample_price(category)
        rows.append(
            {
                "product_id": pid,
                "product_name": product_name,
                "category": category,
                "price": price,
            }
        )
    return pd.DataFrame(rows)

def generate_sales(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    days: int,
    min_tx_per_day: int,
    max_tx_per_day: int,
    fake: Faker,
    start_tx_id: int = 1,
) -> pd.DataFrame:
    rows = []
    now = datetime.utcnow()
    start_date = now.date() - timedelta(days=days - 1)

    tx_id = start_tx_id
    for d in range(days):
        day = start_date + timedelta(days=d)
        tx_for_day = random.randint(min_tx_per_day, max_tx_per_day)

        # Slight regional/category bias by weekday (adds realism)
        weekday = day.weekday()  # 0=Mon
        weekday_region_bias = {
            4: ["NSW", "VIC", "QLD"],  # Fri busier in populous states
            5: ["NSW", "VIC", "QLD"],  # Sat
            6: ["NSW", "VIC", "QLD"],  # Sun
        }
        fav_regions = weekday_region_bias.get(weekday, REGIONS)

        for _ in range(tx_for_day):
            cust = customers.sample(1).iloc[0]
            prod = products.sample(1).iloc[0]

            # Randomize timestamp within the day
            ts = datetime.combine(day, datetime.min.time()) + timedelta(
                seconds=random.randint(8 * 3600, 21 * 3600)  # 8AM–9PM
            )

            # Apply some price variation (discounts / qty)
            qty = max(1, int(np.random.poisson(1.1)))
            amount = round(float(prod["price"]) * qty, 2)

            # Occasionally nudge region to favored ones (to create pattern)
            region = cust["region"]
            if random.random() < 0.25:
                region = random.choice(fav_regions)

            rows.append(
                {
                    "transaction_id": tx_id,
                    "customer_id": int(cust["customer_id"]),
                    "product_id": int(prod["product_id"]),
                    "quantity": qty,
                    "amount": amount,
                    "region": region,
                    "timestamp": ts.isoformat(timespec="seconds"),
                }
            )
            tx_id += 1

    return pd.DataFrame(rows)

def main():
    parser = argparse.ArgumentParser(description="Generate fake retail batch datasets.")
    parser.add_argument("--outdir", default="data", help="Output folder for CSVs")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--customers", type=int, default=1000, help="Number of customers")
    parser.add_argument("--products", type=int, default=200, help="Number of products")
    parser.add_argument("--days", type=int, default=30, help="Number of days of sales history")
    parser.add_argument("--min_tx_day", type=int, default=400, help="Min transactions per day")
    parser.add_argument("--max_tx_day", type=int, default=900, help="Max transactions per day")
    args = parser.parse_args()

    out_path = Path(args.outdir)
    ensure_dir(out_path)
    fake = seeded_faker(args.seed)

    print("▶ Generating customers...")
    customers_df = generate_customers(args.customers, fake)
    print("   customers:", len(customers_df))

    print("▶ Generating products...")
    products_df = generate_products(args.products, fake)
    print("   products:", len(products_df))

    print("▶ Generating sales...")
    sales_df = generate_sales(
        customers_df, products_df, args.days, args.min_tx_day, args.max_tx_day, fake
    )
    print("   sales:", len(sales_df))

    # Write CSVs
    customers_csv = out_path / "customers.csv"
    products_csv = out_path / "products.csv"
    sales_csv = out_path / "sales.csv"

    customers_df.to_csv(customers_csv, index=False)
    products_df.to_csv(products_csv, index=False)
    sales_df.to_csv(sales_csv, index=False)

    print(f"\n✅ Done! Files written to: {out_path.resolve()}")
    print(f"   - {customers_csv.name}  ({len(customers_df)} rows)")
    print(f"   - {products_csv.name}   ({len(products_df)} rows)")
    print(f"   - {sales_csv.name}      ({len(sales_df)} rows)")

if __name__ == "__main__":
    main()