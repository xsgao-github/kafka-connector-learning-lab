# Module 1: Advanced Offset Consumption

## Topic
Manual Offset Control & Point-in-Time Recovery

## Key Kafka Concepts
- **Manual Assignment (`assign`)**: Bypassed Kafka's consumer group coordinator to take direct control over partition assignment.
- **Absolute Seeking (`seek`)**: Implemented the ability to jump to a specific offset in the log, enabling "rewind" capabilities.
- **Time-Travel Seeking (`offsetsForTimes`)**: Integrated the `offsetsForTimes` API to translate wall-clock timestamps into machine-level offsets.
- **Rebalance Coordination**: Implemented a `ConsumerRebalanceListener` to perform custom seeking within the asynchronous lifecycle of a subscribed consumer.

## Experimental Implementations

### 1. Manual Seek (Direct Control)
The most direct way to read from a specific point. It bypasses group management entirely.
```java
// Direct assignment and seek
consumer.assign(partitions);
for (TopicPartition tp : partitions) {
    consumer.seek(tp, offset);
}
```

### 2. Timestamp-Based Seeking (Time-Travel)
Translates a wall-clock timestamp into offsets. This is critical for auditing or recovering data from a known point in time.
```java
// 1. Map partitions to target timestamp
Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
for (TopicPartition tp : partitions) {
    timestampsToSearch.put(tp, timestamp);
}

// 2. Ask Kafka for the offsets corresponding to those timestamps
Map<TopicPartition, OffsetAndTimestamp> foundOffsets = consumer.offsetsForTimes(timestampsToSearch);

// 3. Seek to the returned offsets
for (TopicPartition tp : partitions) {
    OffsetAndTimestamp ot = foundOffsets.get(tp);
    consumer.seek(tp, ot != null ? ot.offset() : endOffset);
}
```

### 3. Rebalance Listener (Group-Based Seek)
Allows using the high-level `subscribe()` API while still maintaining control over the starting offset.
```java
consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // This callback triggers the moment Kafka assigns partitions to this consumer
        for (TopicPartition tp : partitions) {
            consumer.seek(tp, offset);
        }
    }
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
});
```

## Common Pitfalls & Solutions
- **The Async Gap**: Discovered that `subscribe()` is asynchronous. Calling `seek()` immediately after `subscribe()` throws an `IllegalStateException`. **Lesson: Use a `ConsumerRebalanceListener` to time the seek exactly when partitions are assigned.**
- **The `assign` Requirement**: Learned that `assign()` requires a `Collection` of `TopicPartition` objects, even for a single partition. **Lesson: Use `Collections.singletonList()` for targeted single-partition reads.**

## Verification
- **Add New Message**: `python seed_module1.py`
- **Manual Test**: `java -jar ... --module 1 --offset 8`
- **Timestamp Test**: `java -jar ... --module 1 --timestamp "2026-10-10 21:00:00"`
- **Rebalance Test**: `java -jar ... --module 1 --rebalance --offset 8`
- **Expected Result**: Data should start exactly from the specified offset or timestamp, regardless of the consumer group's previous state.
