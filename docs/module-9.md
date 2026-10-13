# Module 9: Kubernetes Readiness — Static Membership & Graceful Shutdown

## Topic
Static Consumer Group Membership & Signal-Driven Graceful Shutdown (`SIGTERM`)

## Key Kafka Concepts
* **Static Membership (`group.instance.id`)**: Assigns a persistent identity to a consumer instance. When a container restarts (e.g., K8s pod rollout), Kafka preserves partition assignments during `session.timeout.ms` (e.g., 45s–5m) without triggering a rebalance storm.
* **Non-Thread-Safe Interruption (`consumer.wakeup()`)**: `KafkaConsumer` is strictly non-thread-safe. The only thread-safe method is `consumer.wakeup()`, which forces `consumer.poll()` to throw `WakeupException`.
* **JVM Shutdown Hook & Signal Catching**: Registering `Runtime.getRuntime().addShutdownHook()` to intercept `SIGTERM` / `SIGINT` (Ctrl+C), initiate `wakeup()`, flush in-flight sink batches, synchronously commit offsets, and close consumer sockets cleanly.
* **Offset Commits vs. Client-Side EOS**: Setting `group.instance.id` and `session.timeout.ms` prevent Kafka server from reblancing, and `commitSync()` prevent consumer from re-read historical data, but EOS still enforced by client side control.
  * **Idempotent Sinks (`UPSERT`)**: Kafka offset commits (`commitSync()`) are **required for correctness** so new pods don't re-read historical topic data from scratch on restart.
  * **DB-Managed Atomic Offsets (`seek()`)**: Kafka offset commits are **required for observability** so monitoring tools don't report 100% false consumer lag.

## Implementation Details

### 1. `GracefulShutdownStrategy.java` (Core Logic Snippet)
Configures static membership and attaches a shutdown hook to handle graceful cleanup on interruption:

```java
// 1. Static Membership & Timeout Bounds
consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "k8s-pod-node-1");
consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000"); // Hold partitions 45s during restart

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

// 2. Register JVM Shutdown Hook
Thread mainThread = Thread.currentThread();
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    System.out.println("\n[Shutdown Hook] SIGTERM/Ctrl+C detected! Calling consumer.wakeup()...");
    consumer.wakeup(); // Thread-safe trigger to unblock poll()
    try {
        mainThread.join();
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
}));

try {
    consumer.subscribe(Collections.singletonList(topicName));
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        // Process batch and commit offsets
        consumer.commitSync();
    }
} catch (WakeupException e) {
    System.out.println("[Module 9] WakeupException caught! Commencing graceful shutdown sequence...");
} finally {
    // 3. Complete in-flight batch, commit offsets, and close cleanly (<50ms network request)
    System.out.println("Flushing in-flight buffers, committing offsets, and closing consumer cleanly.");
    consumer.commitSync();
    consumer.close();
}
```

### 2. `seed_module9.py` (Stream Seeder)
Seeds stream records to verify continuous consumption and offset commits before and during process termination.

## Common Pitfalls & Solutions
* **Static Membership Lockout (`UnbindableInstanceException`)**: If a second container starts with the exact same `group.instance.id` while the first is still active, Kafka rejects the new instance. Ensure `group.instance.id` incorporates unique pod identity env vars (e.g., `POD_NAME` from Kubernetes StatefulSets) or set `session.timeout.ms` appropriately.
* **`commitSync()` is Still Needed with Client-Side EOS**: DB handles deduplication, but Kafka `commitSync()` prevents re-reading historical topics from zero on restart, it also ensures Kafka metrics/alerts don't report 100% unread consumer lag.
* **Grace Period Window**: `commitSync()` takes <50ms, easily completing within K8s default `terminationGracePeriodSeconds` (30s) before `SIGKILL`.

## Verification
* **Start Consumer**: Run app with `--module 9` parameter
* **Seed Records**: `python .\scripts\seed_module9.py`
* **Trigger Graceful Shutdown**: Send `Ctrl+C` (or `kill -15 <pid>`). Verify that `WakeupException` is caught, final offsets are committed, and the socket closes cleanly without offset gaps or rebalance storms.
