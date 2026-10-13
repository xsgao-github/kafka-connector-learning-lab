from kafka_utils import send_string_records, USERS_TOPIC

def seed_module6_data():
    # 1. Produce V1 records without header flag
    v1_records = [
        "11:11,Kevin,110.0",
        "12:12,Laura,120.0"
    ]
    send_string_records(USERS_TOPIC, v1_records, with_header=False)

    # 2. Produce V2 and V3 records with header flag enabled
    v2_v3_records = [
        "schema_version:v2\t13:13,Mark,130.0,mark@example.com",
        "schema_version:v2\t14:14,Nina,140.0,nina@example.com",
        "schema_version:v3\t15:15,Oscar,150.0,oscar@example.com,555-0150",
        "schema_version:v3\t16:16,Paula,160.0,paula@example.com,555-0160"
    ]
    send_string_records(USERS_TOPIC, v2_v3_records, with_header=True)

if __name__ == "__main__":
    seed_module6_data()