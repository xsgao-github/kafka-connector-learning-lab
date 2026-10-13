import time
from kafka_utils import send_string_records, USERS_TOPIC

def seed_module9_data(count=100):
    """
    Seeds a stream of records for testing Module 9 static membership
    and graceful shutdown signal handling.
    """
    print(f"=== [Module 9 Data Seeder] Publishing {count} test records to '{USERS_TOPIC}' ===")
    records = []
    for i in range(1, count + 1):
        user_id = 3000 + i
        records.append(f"schema_version:v3\t{user_id}:{user_id},User_{i},{i * 3.0:.2f},user{i}@example.com,555-{i:04d}")

    sent = send_string_records(USERS_TOPIC, records, with_header=True)
    print(f"[Module 9 Data Seeder] Published {sent}/{count} records successfully.")

if __name__ == "__main__":
    seed_module9_data(100)
