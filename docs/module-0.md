# Module 0: Getting Started

## Topic
Baseline Connectivity & Basic Consumption

## Key Kafka Concepts
- **Kafka Connectivity**: Established communication between a Java application and a Kafka broker using the `KafkaConsumer` API.
- **Consumption Model**: Implemented the high-level `subscribe()` API for automatic partition assignment and consumer group management.
- **Offset Reset Policy**: Configured `AUTO_OFFSET_RESET_CONFIG` to `earliest` to ensure the consumer reads the topic from the beginning if no committed offset exists.

## Template Code

### Connection Setup
The `KafkaReader` initializes the connection properties required by the Kafka client:
```java
this.consumerProps = new Properties();
// Host/port pairs for initial cluster connection
this.consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
// Unique ID for the consumer group
this.consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-connector-group-" + UUID.randomUUID());
// Deserializer for message keys and values
this.consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
this.consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
// Start from the beginning if no offset exists
this.consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
```

### Consumption Logic
The `read` method handles the subscription and the polling loop:
```java
try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
    // Subscribe to the topic using Kafka's high-level consumer API
    consumer.subscribe(Collections.singletonList(topic));

    while (results.size() < limit) {
        // Poll Kafka for new records, waiting up to 1 second if none are available
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
            results.add(convertMessageToRow(record.value(), targetSchema));
        }
    }
}
```

## Verification
- **Environment**: Run `python setup_env.py`.
- **Execution**: Run `java -jar target/kafka-connector-learning-lab-1.0-SNAPSHOT.jar --metadata metadata.json --bootstrap localhost:9092 --module 0`.
- **Expected Result**: Successful reading of seeded users from the `users_topic`.
