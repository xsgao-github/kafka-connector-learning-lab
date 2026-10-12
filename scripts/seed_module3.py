from kafka_utils import send_string_records, USERS_TOPIC

def seed_module3_data():
    module3_records = [
        "7:George,70.0",
        "8:8,POISON_PILL_CORRUPTED_RECORD",
        "9:9,CORRUPTED_PAYLOAD_INVALID_FORMAT",
        "10:10,Julia,100.0"
    ]

    print(f"--- Appending Module 3 Records (11+: Kevin, Poison Pills, Laura, etc.) to '{USERS_TOPIC}' ---")
    sent_count = send_string_records(USERS_TOPIC, module3_records)
    print(f"Successfully produced {sent_count}/5 records starting after 10/Julia.")

if __name__ == "__main__":
    seed_module3_data()