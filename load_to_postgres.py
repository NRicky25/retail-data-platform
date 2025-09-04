import os, sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# PG_HOST = os.getenv("PGHOST", "localhost")
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB   = os.getenv("PG_DB",   "retail")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "admin")

DATA_DIR = Path("data")
CUSTOMERS = DATA_DIR / "customers.csv"
PRODUCTS = DATA_DIR / "products.csv"
SALES = DATA_DIR / "sales.csv"

DDL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    name TEXT, email TEXT, region TEXT, join_date DATE
);
CREATE TABLE IF NOT EXISTS products(
    product_id INT PRIMARY KEY,
    product_name TEXT, category TEXT, price NUMERIC(10,2)
);
CREATE TABLE IF NOT EXISTS sales(
    transaction_id BIGINT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    quantity INT,
    amount NUMERIC(12, 2),
    region TEXT,
    timestamp TIMESTAMP
);
"""
TRUNC = """
TRUNCATE TABLE sales, products, customers
RESTART IDENTITY
CASCADE;
"""
INDEXES = """
CREATE INDEX IF NOT EXISTS idx_sales_ts ON sales(timestamp);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_customers_region ON customers(region);
"""

def copy_csv(cur, table, path, cols):
    if not Path(path).exists():
        raise FileNotFoundError(f"Missing CSV: {path}")
    with open(path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, HEADER true)", f
        )

def main():
    for p in (CUSTOMERS, PRODUCTS, SALES):
        if not p.exists():
            print(f"Missing:", p); sys.exit(1)

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB
    )
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(TRUNC)
            copy_csv(cur, "customers", CUSTOMERS, ["customer_id","name","email","region","join_date"])
            copy_csv(cur, "products",  PRODUCTS,  ["product_id","product_name","category","price"])
            copy_csv(cur, "sales",     SALES,     ["transaction_id","customer_id","product_id","quantity","amount","region","timestamp"])
            cur.execute(INDEXES)
            cur.execute("select inet_server_addr(), inet_server_port(), current_database(), current_user, current_schema();")
            conn.commit()
            print("Loaded customers, products, and sales into Postgres.")
            print("Connected to:", cur.fetchone())
    except Exception as e:
        conn.rollback()
        print("Error, rolled back:", e); raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()

