# Module 8: Flow Control & Backpressure (`pause()` / `resume()`)

## Topic
Dynamic Consumer Flow Control & Watermark Hysteresis

## Key Kafka Concepts
* **Heartbeat & Liveness Retention**: Continuous execution of `consumer.poll()` prevents `max.poll.interval.ms` timeout evictions and consumer group rebalances during slow downstream processing.
* **Pause / Resume Semantics**: Calling `consumer.pause(partitions)` stops record fetching from Kafka while maintaining active liveness polling; calling `consumer.resume(partitions)` restores record fetching.
* **Watermark Hysteresis**: Utilizing dual buffer thresholds—a **High Watermark** (`1000` records) to trigger pause and a **Low Watermark** (`200` records) to trigger resume—preventing rapid oscillation (thrashing) between states.

## Implementation Details

### 1. `BackpressureStrategy.java` (Core Logic Snippet)
Monitors internal buffer backlog, applying dynamic pause and resume controls while maintaining `poll()` heartbeats:

```java
// 1. ALWAYS poll to maintain group liveness (prevents max.poll.interval.ms eviction)
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
Set<TopicPartition> assignedPartitions = consumer.assignment();

currentBufferBacklog += records.count();

// 2. High Watermark Backpressure (Pause fetching, keep polling active)
if (!isPaused && currentBufferBacklog >= HIGH_WATERMARK_BUFFER && !assignedPartitions.isEmpty()) {
    consumer.pause(assignedPartitions);
    isPaused = true;
    System.err.printf("[BACKPRESSURE ALERT] Buffer backlog (%d >= %d). PAUSING partitions.%n",
            currentBufferBacklog, HIGH_WATERMARK_BUFFER);
}

// 3. Low Watermark Recovery (Resume fetching)
if (isPaused && currentBufferBacklog <= LOW_WATERMARK_BUFFER && !assignedPartitions.isEmpty()) {
    consumer.resume(assignedPartitions);
    isPaused = false;
    System.out.printf("[BACKPRESSURE RESUME] Buffer drained (%d <= %d). RESUMING partitions.%n",
            currentBufferBacklog, LOW_WATERMARK_BUFFER);
}
```

### 2. `seed_module8.py` (Traffic Surge Generator)
Produces a 3,000-record burst in rapid 500-record batches to fill the consumer's buffer and force backpressure state transitions.

## Common Pitfalls & Solutions
* **`max.poll.interval.ms` Rebalance Eviction**: Stopping `poll()` while waiting for a slow sink causes the broker to declare the consumer dead and trigger group rebalances. Fix: continue executing `consumer.poll()` in the main loop after calling `pause()`. Kafka returns 0 records while maintaining heartbeat liveness.
* **State Thrashing**: Different partitions may return records in different speed. Use a single numbers to capture the backlog records (fetched but not processed yet) across partitions, pause polling when it reaches High Watermark (`1000`) and resume when it reaches Low Watermark (`200`).
* **Backpressure vs. Offset Commits**: Even during paused fetching, offset commits for already processed records can proceed safely without affecting buffer pressure.

## Verification
* **Start Consumer (Terminal 1)**: Run app with `--module 8` parameter
* **Trigger Traffic Surge (Terminal 2)**: `python .\scripts\seed_module8.py`
* **Observe Flow Control**: Verify log output shows `PAUSING partitions` when backlog hits `1000`, followed by `RESUMING partitions` once backlog drains to `200`.
