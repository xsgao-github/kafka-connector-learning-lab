from kafka_utils import send_string_records, USERS_TOPIC

def seed_module1_data():
    module1_records = [
        "5:5,Evan,50.0",
        "6:6,Fiona,60.0"
    ]

    print(f"--- Appending Module 1 Records (6-7: Fiona, George) to '{USERS_TOPIC}' ---")
    sent_count = send_string_records(USERS_TOPIC, module1_records)
    print(f"Successfully produced {sent_count}/2 additional records.")

if __name__ == "__main__":
    seed_module1_data()