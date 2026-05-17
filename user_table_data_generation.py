import psycopg2
from faker import Faker
from datetime import datetime, timedelta
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

USER_IDS = [f"USER_{i}" for i in range(1, 1001)]

COUNTRIES = ["CA", "US"]

# ---------- CONNECT ----------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ---------- GENERATE DATA ----------
users_data = []

for user_id in USER_IDS:
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}@example.com"
    country = random.choice(COUNTRIES)

    created_at = fake.date_time_between(
        start_date="-2y",
        end_date="now"
    )

    users_data.append((
        user_id,
        first_name,
        last_name,
        email,
        country,
        created_at
    ))

# ---------- INSERT ----------
insert_query = """
INSERT INTO delivery_app_info.users (user_id, first_name, last_name, email, country, created_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO NOTHING;
"""

cur.executemany(insert_query, users_data)

conn.commit()

print(f"Inserted {len(users_data)} users")

# ---------- CLOSE ----------
cur.close()
conn.close()