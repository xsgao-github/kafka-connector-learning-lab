import time
from kafka_utils import send_string_records, USERS_TOPIC

def seed_module7_traffic_burst(count=2000, batch_size=500):
    """
    Generates a burst of records to simulate high throughput traffic
    and induce visible consumer lag for Module 7 observability testing.
    """
    print(f"=== [Module 7 Traffic Generator] Preparing {count} records for '{USERS_TOPIC}' ===")
    
    all_records = []
    for i in range(1, count + 1):
        user_id = 1000 + i
        # Seed records with v3 schema header formatting
        record_str = f"schema_version:v3\t{user_id}:{user_id},User_{i},{i * 1.5:.2f},user{i}@example.com,555-{i:04d}"
        all_records.append(record_str)

    total_sent = 0
    start_time = time.time()

    # Send in chunks to simulate rapid batch ingestion
    for chunk_start in range(0, count, batch_size):
        chunk = all_records[chunk_start:chunk_start + batch_size]
        print(f"--> Publishing traffic burst ({chunk_start + 1} to {chunk_start + len(chunk)})...")
        sent = send_string_records(USERS_TOPIC, chunk, with_header=True)
        total_sent += sent
        time.sleep(0.2)  # Brief delay between bursts to make lag progression visible

    elapsed = time.time() - start_time
    print(f"\n[Module 7 Traffic Generator] Done! Published {total_sent}/{count} records in {elapsed:.2f}s.")
    print("Check your running Module 7 consumer terminal to observe lag spikes and real-time draining!")

if __name__ == "__main__":
    seed_module7_traffic_burst(count=2000, batch_size=500)