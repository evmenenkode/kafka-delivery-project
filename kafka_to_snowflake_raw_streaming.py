import json
from kafka import KafkaConsumer
import snowflake.connector
from config import SNOWFLAKE_CONFIG

# ---------- KAFKA ----------
consumer = KafkaConsumer(
    "raw_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=None
)

# ---------- SNOWFLAKE ----------
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")

print("Starting consumer...")

BATCH_SIZE = 10
batch = []

try:
    for message in consumer:
        event = message.value

        print(f"Got event: {event.get('event_id')}")

        batch.append(json.dumps(event))

        if len(batch) >= BATCH_SIZE:
            # строим SELECT UNION ALL
            select_statements = " UNION ALL ".join(
                ["SELECT PARSE_JSON(%s)"] * len(batch)
            )

            insert_query = f"""
                INSERT INTO raw_events (raw_data)
                {select_statements}
            """

            cursor.execute(insert_query, batch)

            print(f"Inserted batch of {len(batch)} events")

            batch.clear()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    if batch:
        select_statements = " UNION ALL ".join(
            ["SELECT PARSE_JSON(%s)"] * len(batch)
        )

        insert_query = f"""
            INSERT INTO raw_events (raw_data)
            {select_statements}
        """

        cursor.execute(insert_query, batch)

        print(f"Inserted final batch of {len(batch)} events")

    cursor.close()
    conn.close()