# Module 7: Observability (Consumer Lag & Metrics)

## Topic
Active Consumer Lag Computation & Telemetry Monitoring

## Key Kafka Concepts
* **Partition High Watermark (Log End Offset)**: Querying broker-side `endOffsets(partitions)` to determine the latest available record offset across topic partitions.
* **Read Position Lag Calculation**: Computing exact consumer lag per partition using `logEndOffset - consumer.position(partition)` based on client-side read positions.
* **Native Kafka Metrics**: Programmatically inspecting `consumer.metrics()` to extract built-in telemetry such as `records-lag-max` and fetch latency metrics.
* **Sink Latency Simulation & Lag Accumulation**: Simulating downstream write delays to observe real-time unread record accumulation during message traffic bursts.
* **Alerting Thresholds**: Emitting real-time warnings to `System.err` when calculated lag exceeds operational safety limits.

## Implementation Details

### 1. `ObservabilityStrategy.java` (Core Logic Snippet)
Computes partition-level lag against read positions, inspects native metrics, simulates processing latency, and triggers alerts on high lag:

```java
// 1. Simulate downstream sink write latency
if (batchSize > 0) {
    Thread.sleep((long) batchSize * SIMULATED_RECORD_DELAY_MS);
}

// 2. Compute Lag per assigned partition: LogEndOffset - Read Position
long totalLag = 0;
if (!assignedPartitions.isEmpty()) {
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignedPartitions);
    for (TopicPartition tp : assignedPartitions) {
        long currentPosition = consumer.position(tp);
        long logEndOffset = endOffsets.getOrDefault(tp, currentPosition);
        totalLag += Math.max(0, logEndOffset - currentPosition);
    }
}

// 3. Query native Kafka consumer metrics
double nativeMaxLag = getNativeMetricValue(consumer, "records-lag-max");

// 4. Alert via System.err if total lag exceeds threshold
if (totalLag > LAG_THRESHOLD) {
    System.err.printf("[ALERT][Module 7] High Consumer Lag Detected! Unread messages: %d (Threshold: %d)%n",
            totalLag, LAG_THRESHOLD);
}
```

### 2. `seed_module7.py` (Traffic Generator Snippet)
Produces a 2,000-record traffic burst in 500-message chunks to induce visible consumer lag:

```python
def seed_module7_traffic_burst(count=2000, batch_size=500):
    all_records = [
        f"schema_version:v3\t{1000+i}:{1000+i},User_{i},{i*1.5:.2f},user{i}@example.com,555-{i:04d}"
        for i in range(1, count + 1)
    ]
    for chunk_start in range(0, count, batch_size):
        chunk = all_records[chunk_start:chunk_start + batch_size]
        send_string_records(USERS_TOPIC, chunk, with_header=True)
        time.sleep(0.2)
```

## Common Pitfalls & Solutions

* **Polling speed is much faster the pushing (`Calculated Lag: 0`)**: High-speed in-memory polling drained the consumer queue so quickly that offset comparisons executed after all messages were already consumed, showing 0 lag. Introduced simulated sink processing latency (`Thread.sleep(batchSize * 5ms)`) to model real-world database ingestion delay and allow unread messages to accumulate on the broker.

#### Verification
* **Start Long-Running Consumer (Terminal 1)**: Run app with `--module 7` parameter.
* **Trigger Traffic Burst (Terminal 2)**: `python .\scripts\seed_module7.py`
* **Observe Alert & Drain (Terminal 1)**:  Observe `Calculated Lag` spiking to 1500, triggering `[ALERT][Module 7] High Consumer Lag Detected! Unread messages: 500 (Threshold: 300)` on `System.err`, and smoothly draining down to `0` as batches finish processing.
