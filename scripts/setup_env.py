from kafka_utils import (
    delete_topics_if_exist,
    ensure_topic_exists,
    send_string_records,
    USERS_TOPIC,
    USERS_DLQ_TOPIC,
    ORDERS_TOPIC,
    ORDERS_DLQ_TOPIC,
)

def setup_environment():
    print(f"--- Resetting & Initializing Kafka Environment ('{USERS_TOPIC}, {ORDERS_TOPIC}') ---")
    
    # 1. Safely clear existing topics without throw errors if absent
    delete_topics_if_exist([USERS_TOPIC, USERS_DLQ_TOPIC, ORDERS_TOPIC, ORDERS_DLQ_TOPIC])

    # 2. Re-create clean topics
    ensure_topic_exists(USERS_TOPIC)
    ensure_topic_exists(USERS_DLQ_TOPIC)
    ensure_topic_exists(ORDERS_TOPIC)
    ensure_topic_exists(ORDERS_DLQ_TOPIC)

    # 3. Seed baseline 5 records (IDs 1-5)
    initial_records = [
        "1:1,Alice,10.0",
        "2:2,Bob,20.0",
        "3:3,Charlie,30.0",
        "4:4,Diana,40.0"
    ]

    sent_count = send_string_records(USERS_TOPIC, initial_records)
    print(f"Environment reset complete. Seeded {sent_count} baseline records to '{USERS_TOPIC}'.")

if __name__ == "__main__":
    setup_environment()