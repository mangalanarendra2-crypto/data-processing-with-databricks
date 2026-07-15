# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Generate Sample Data
# MAGIC
# MAGIC Generates a synthetic raw "orders" dataset (with some intentionally messy
# MAGIC records — nulls, duplicates, bad values) and writes it as CSV so the
# MAGIC Bronze notebook has something realistic to ingest.

# COMMAND ----------

import os
import random
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.config import RAW_DATA_PATH, SAMPLE_NUM_ORDERS, SAMPLE_SEED  # noqa: E402

try:
    from faker import Faker
except ImportError:
    Faker = None

# COMMAND ----------

random.seed(SAMPLE_SEED)
fake = Faker() if Faker else None
if fake:
    Faker.seed(SAMPLE_SEED)

REGIONS = ["North", "South", "East", "West", "Central"]
PRODUCTS = [
    ("P001", "Wireless Mouse", 19.99),
    ("P002", "Mechanical Keyboard", 89.99),
    ("P003", "USB-C Hub", 34.50),
    ("P004", "27in Monitor", 249.00),
    ("P005", "Webcam HD", 59.99),
    ("P006", "Laptop Stand", 45.00),
    ("P007", "Noise Cancelling Headphones", 199.99),
    ("P008", "Desk Lamp", 24.99),
]

# COMMAND ----------


def random_customer_name(i: int) -> str:
    if fake:
        return fake.name()
    return f"Customer {i}"


def generate_orders(num_orders: int):
    rows = []
    for i in range(num_orders):
        product_id, product_name, price = random.choice(PRODUCTS)
        quantity = random.randint(1, 5)

        # Inject some messy data to make the Silver layer's job meaningful.
        customer_name = random_customer_name(i)
        if random.random() < 0.02:
            customer_name = None  # missing customer name

        region = random.choice(REGIONS)
        if random.random() < 0.01:
            region = None  # missing region

        quantity_val = quantity
        if random.random() < 0.01:
            quantity_val = -1  # invalid negative quantity

        order_id = f"ORD{i:06d}"
        # Duplicate ~1% of order IDs to simulate upstream duplication.
        if random.random() < 0.01 and rows:
            order_id = rows[-1]["order_id"]

        rows.append(
            {
                "order_id": order_id,
                "customer_name": customer_name,
                "region": region,
                "product_id": product_id,
                "product_name": product_name,
                "unit_price": price,
                "quantity": quantity_val,
                "order_date": fake.date_between(start_date="-90d", end_date="today").isoformat()
                if fake
                else "2026-06-01",
            }
        )
    return rows


# COMMAND ----------

if __name__ == "__main__":
    import csv

    orders = generate_orders(SAMPLE_NUM_ORDERS)
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    out_file = os.path.join(RAW_DATA_PATH, "orders.csv")

    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(orders[0].keys()))
        writer.writeheader()
        writer.writerows(orders)

    print(f"Generated {len(orders)} sample orders at: {out_file}")
