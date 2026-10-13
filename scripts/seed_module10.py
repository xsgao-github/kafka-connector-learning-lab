from kafka_utils import send_string_records, delete_topics_if_exist, ensure_topic_exists

ORDERS_TOPIC = "orders_topic"

def seed_module10_orders():
    orders = [
        "1:o501,u101,149.99,COMPLETED",
        "2:o502,u102,89.50,PENDING",
        "3:o503,u103,220.00,SHIPPED",
        "4:o504,u101,45.25,COMPLETED",
        "5:o505,u104,12.00,CANCELLED",
    ]

    print(f"--> Publishing 5 order messages to '{ORDERS_TOPIC}'...")
    sent = send_string_records(ORDERS_TOPIC, orders)
    print(f"\n[Module 10 Generator] Done! Successfully published {sent}/5 order messages.")

if __name__ == "__main__":
    seed_module10_orders()