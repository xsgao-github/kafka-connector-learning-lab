import time
from kafka_utils import send_string_records, USERS_TOPIC

def seed_module8_traffic_surge(count=3000, batch_size=500):
    """
    Generates a rapid burst of 3,000 records to induce buffer backpressure
    and trigger pause()/resume() flow control in Module 8.
    """
    print(f"=== [Module 8 Traffic Generator] Preparing {count} records for '{USERS_TOPIC}' ===")
    
    all_records = []
    for i in range(1, count + 1):
        user_id = 2000 + i
        record_str = f"schema_version:v3\t{user_id}:{user_id},User_{i},{i * 2.5:.2f},user{i}@example.com,555-{i:04d}"
        all_records.append(record_str)

    total_sent = 0
    start_time = time.time()

    for chunk_start in range(0, count, batch_size):
        chunk = all_records[chunk_start:chunk_start + batch_size]
        print(f"--> Surge: Publishing batch ({chunk_start + 1} to {chunk_start + len(chunk)})...")
        sent = send_string_records(USERS_TOPIC, chunk, with_header=True)
        total_sent += sent
        time.sleep(0.1)  # Minimal delay to rapidly build buffer pressure

    elapsed = time.time() - start_time
    print(f"\n[Module 8 Traffic Generator] Done! Published {total_sent}/{count} records in {elapsed:.2f}s.")

if __name__ == "__main__":
    seed_module8_traffic_surge(count=3000, batch_size=500)
