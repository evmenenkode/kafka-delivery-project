import psycopg2
from faker import Faker
from datetime import datetime
import random

fake = Faker()

# ---------- CONFIG ----------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "analytics_db",
    "user": "analytics_user",
    "password": "analytics_pass"
}

PRODUCT_IDS = [f"PRODUCT_{i}" for i in range(1, 101)]

CATEGORIES = [
    "electronics",
    "clothing",
    "home",
    "sports",
    "beauty",
    "toys",
    "food"
]

# ---------- CONNECT ----------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ---------- GENERATE DATA ----------
products_data = []

for product_id in PRODUCT_IDS:
    category = random.choice(CATEGORIES)

    # Генерируем название продукта
    product_name = f"{fake.word().capitalize()} {category.capitalize()} Item"

    # Реалистичные цены по категориям
    if category == "electronics":
        price = round(random.uniform(100, 2000), 2)
    elif category == "clothing":
        price = round(random.uniform(20, 200), 2)
    elif category == "home":
        price = round(random.uniform(30, 500), 2)
    elif category == "sports":
        price = round(random.uniform(25, 300), 2)
    elif category == "beauty":
        price = round(random.uniform(10, 150), 2)
    elif category == "toys":
        price = round(random.uniform(5, 100), 2)
    else:  # food
        price = round(random.uniform(5, 50), 2)

    created_at = fake.date_time_between(
        start_date="-2y",
        end_date="now"
    )

    products_data.append((
        product_id,
        product_name,
        category,
        price,
        created_at
    ))

# ---------- INSERT ----------
insert_query = """
INSERT INTO delivery_app_info.products (product_id, product_name, category, price, created_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (product_id) DO NOTHING;
"""

cur.executemany(insert_query, products_data)

conn.commit()

print(f"Inserted {len(products_data)} products")

# ---------- CLOSE ----------
cur.close()
conn.close()