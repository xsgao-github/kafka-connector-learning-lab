# Module 10: Consumer Pool

## Topic
True Object Pooling Pattern for Kafka Consumers Scoped Per Topic & Client Delegation Pattern

## Key Kafka Concepts
* **Per-Topic Consumer Object Pools**: Bypasses dynamic topic subscription switching by pre-allocating dedicated `BlockingQueue` pools of `KafkaConsumer` instances for each topic (e.g., `users_topic`, `orders_topic`).
* **Pre-Allocated Worker Execution Pool**: Runs worker threads via an `ExecutorService` that dynamically acquire a consumer from a topic pool, execute polling tasks, and return the consumer to its pool upon completion.
* **Client Handler Delegation**: Decouples the connector framework's polling infrastructure from downstream processing logic by delegating raw `ConsumerRecords` directly to a client functional callback (`Consumer<ConsumerRecords<String, String>>`).
* **Graceful Lifecycle & Shutdown Hooks**: Registers JVM shutdown hooks to trigger `consumer.wakeup()` across all pooled consumers, ensuring in-flight offsets are committed synchronously via `commitSync()` during `SIGTERM`.

## Implementation Details

### 1. Pre-Allocated Consumer Pools per Topic
Instead of creating temporary consumer instances or dynamically switching topic subscriptions on a single consumer, `read()` initializes a dedicated `ArrayBlockingQueue<KafkaConsumer>` object pool for each discovered topic:

```java
// Pre-allocate Consumer Pools for each discovered topic
for (String topic : targetTopics) {
    BlockingQueue<KafkaConsumer<String, String>> pool = new ArrayBlockingQueue<>(POOL_SIZE_PER_TOPIC);
    for (int i = 0; i < POOL_SIZE_PER_TOPIC; i++) {
        Properties props = (Properties) consumerProps.clone();
        props.put("client.id", "pool-consumer-" + topic + "-" + (i + 1));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        pool.add(consumer); // Hold pre-subscribed consumer in topic pool
    }
    topicConsumerPools.put(topic, pool);
}
```

### 2. Acquire -> Poll -> Delegate -> Return Lifecycle
Worker threads acquire a consumer from the topic's pool, execute the polling loop, and return the consumer to the pool in a `finally` block:

```java
private void executeTopicPollingTask(
        String topic,
        Consumer<ConsumerRecords<String, String>> clientHandler) {

    BlockingQueue<KafkaConsumer<String, String>> pool = topicConsumerPools.get(topic);
    if (pool == null) return;

    KafkaConsumer<String, String> consumer = null;
    try {
        // 1. Acquire consumer from topic pool
        consumer = pool.poll(5, TimeUnit.SECONDS);
        if (consumer == null) return;

        while (!Thread.currentThread().isInterrupted()) {
            // 2. Poll Kafka
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            if (!records.isEmpty()) {
                // 3. Delegate raw records directly to client handler
                clientHandler.accept(records);
                consumer.commitAsync();
            }
        }
    } catch (WakeupException e) {
        System.out.printf("[%s] Interrupted by shutdown.%n", topic);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    } finally {
        if (consumer != null) {
            try {
                // 4. Synchronous commit on exit
                consumer.commitSync();
            } catch (Exception ignored) {
            } finally {
                // 5. Return consumer back to topic pool
                pool.offer(consumer);
            }
        }
    }
}
```

## Common Pitfalls & Solutions
* **Rebalance Storms from Topic Switching**: Dynamically calling `unsubscribe()` and `subscribe()` to switch a consumer between topics causes repeated rebalance delays, socket resets, and internal buffer dumps. Pre-allocate dedicated consumer object pools per topic so consumer instances remain statically subscribed to a single topic throughout their lifecycle.
* **Consumer Object Leaks on Execution Exceptions**: Unhandled exceptions during worker processing can prevent consumers from returning to their object pool, exhausting available consumers. Always enclose worker polling execution in a `try-finally` block that guarantees `pool.offer(consumer)` is called on exit.

## Verification
* **Seed Order Records**: Run `python seed_module10-v2.py`.
* **Execute Module 10**: Run app with `--module 10` parameter.
* **Expected Result**: Console logs confirm the two workers, `users_topic` and `orders_topic`, polling records indpendedly.
