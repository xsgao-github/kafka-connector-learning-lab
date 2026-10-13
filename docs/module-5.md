# Module 5: Delivery Guarantees & Offset Management

## Topic
At-Most-Once, At-Least-Once, and Exactly-Once Semantics

## Key Kafka Concepts
- **Auto-Commit Mechanism**: Controlled by `enable.auto.commit` and `auto.commit.interval.ms`. Understand how the background thread handles offset commits.
- **Server-side Tracking**: Using Kafka's internal `__consumer_offsets` topic to store the current progress of a consumer group.
- **Client-side Tracking**: Explicitly managing partition id and offsets (e.g., in a database) to ensure atomic operations between data processing and offset updates.
- **Idempotency**: The foundation for Exactly-Once, ensuring that processing the same message multiple times doesn't result in side effects.

## Experimental Implementations

### 1. At-Most-Once (Fire and Forget)
The consumer commits the offset as soon as the message is polled, before processing.
```java
// enable.auto.commit = true
// Or manual commit right after poll
consumer.poll(Duration.ofMillis(100));
consumer.commitSync(); // Commit before processing logic
process(records);
```

### 2. At-Least-Once (Standard ETL)
The consumer processes the message fully before committing the offset.
```java
// enable.auto.commit = false
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        process(record);
    }
    consumer.commitSync(); // Commit only after successful processing
}
```

### 3. Exactly-Once (EOS)
The most complex pattern, requiring atomic commits or idempotent processing.
```java
// Pattern A: Transactional Producer - Kafka-to-Kafka
producer.initTransactions();
producer.beginTransaction();
// ... send data ...
producer.commitTransaction();

// Pattern B: Atomic DB Update (Client-side tracking)
// This ensures that the data and the offset are updated in a single atomic transaction.
// 1. Start DB Transaction
dbConnection.beginTransaction();
try {
    for (ConsumerRecord<String, String> record : records) {
        // 2. Write data to DB
        dbConnection.execute("INSERT INTO target_table ...", record.value());
    }
    
    // 3. Write offset to a 'metadata' table in same DB
    dbConnection.execute("UPDATE metadata SET offset = ? WHERE partition_id = ?", 
                          record.offset(), record.partition());
    
    // 4. Commit DB Transaction
    dbConnection.commit();
} catch (Exception e) {
    dbConnection.rollback();
}

// 5. On restart, fetch the offset from the DB and seek
Long lastOffset = dbConnection.query("SELECT offset FROM metadata WHERE partition_id = ?", partitionId);
consumer.assign(partitions);
consumer.seek(new TopicPartition(topic, partitionId), lastOffset);
```

## Delivery Guarantees Comparison

| Feature / Attribute | At-Most-Once | At-Least-Once | Exactly-Once (EOS) |
| :--- | :--- | :--- | :--- |
| `enable.auto.commit` | true (or early manual commit) | false | false |
| Commit Timing | Before / During record processing | Strictly After record processing | Atomically Paired with target DB transaction |
| Commit Mechanism | Background timer (auto.commit.interval.ms) or immediate manual commit after .poll() | Explicit consumer.commitSync() or checkpoint update after DB write completes | Kafka Transactional Producer API or atomic DB offset store + seek() |
| Data Loss Risk | Possible (if app crashes after offset commit but before processing completes) | Zero | Zero |
| Duplicate Risk | Zero | Possible (if app crashes after processing completes but before offset commit) | Zero |
| System Overhead | Minimal | Low | Moderate (Transactions, 2PC, or Idempotency checks) |
| Primary Use Cases | High-throughput telemetry, log streaming, non-critical metrics | General ETL pipelines, streaming analytics, event processing | Financial transactions, billing, inventory control |

## Common Pitfalls & Solutions
- **The Auto-Commit Trap**: Set `enable.auto.commit = false`—background timers commit unpredictably and cause race conditions.
- **Consumer Group Isolation**: Use `mode-scoped group IDs` (kafka-connector-group-<mode>) so delivery modes don't overwrite each other's offsets.
- **The Zombie Consumer**: Exceeding `max.poll.interval.ms` triggers rebalances—tune poll limits or processing speed to avoid duplicate processing.
- **External State Alignment**: Store (`partition, offset`) atomically alongside target database rows to power `consumer.seek()` recovery.

## Verification
- **At-Most-Once**: Run with `--module 5 --mode at_most_once --crash` (commits on poll, crashes mid-batch), then run without `--crash` again (verifies mid-batch records were permanently lost/skipped).
- **At-Least-Once**: Run with `--module 5 --mode at_least_once --crash` (processes records, crashes before commitSync()), then run without `--crash` twice (verifies mid-batch records are re-read and duplicated).
- **EOS**: Run with `--module 5 --mode exact_once --crash` (checkpoints offset to external store before crash), then run without `--crash` 1–2 times (verifies seek() resumes seamlessly with zero loss and zero duplicates).
