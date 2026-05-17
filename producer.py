import json
import random
import time
import uuid
from datetime import datetime, timedelta, UTC
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "raw_events"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
PLATFORMS = ["web", "ios", "android"]
COUNTRIES = ["CA", "US"]
USER_IDS = [f"USER_{i}" for i in range(1, 1001)]
PRODUCT_IDS = [f"PRODUCT_{i}" for i in range(1, 101)]

def random_timestamp_last_6_days():
    now = datetime.now(UTC)
    past = now - timedelta(days=6)

    random_seconds = random.uniform(0, (now - past).total_seconds())
    return (past + timedelta(seconds=random_seconds)).isoformat()

def generate_event():
    is_invalid = random.random() < 0.25

    user_id = random.choice(USER_IDS)
    product_id = random.choice(PRODUCT_IDS)
    event_type = random.choice(EVENT_TYPES)

    amount = round(random.uniform(10, 500), 2)
    currency = "CAD"

    session_id = str(uuid.uuid4())
    platform = random.choice(PLATFORMS)
    country = random.choice(COUNTRIES)

    invalid_field = None
    if is_invalid:
        invalid_field = random.choice([
            "user_id",
            "event_type",
            "amount",
            "currency"
        ])

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": None if invalid_field == "user_id" else user_id,
        "session_id": session_id,
        "event_type": None if invalid_field == "event_type" else event_type,
        "amount": None if invalid_field == "amount" else amount,
        "currency": None if invalid_field == "currency" else currency,
        "product_id": product_id,
        "platform": platform,
        "country": country,
        "event_timestamp": random_timestamp_last_6_days(),
        "is_valid": not is_invalid
    }

    return event["user_id"], event


print("Starting Kafka producer...")

try:
    while True:
        key, event = generate_event()

        future = producer.send(
            topic=TOPIC_NAME,
            key=key,
            value=event
        )

        future.get(timeout=10)

        print(
            f"Produced event | user={key} | type={event['event_type']} | valid={event['is_valid']}"
        )

        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping producer...")

finally:
    producer.flush()
    producer.close()