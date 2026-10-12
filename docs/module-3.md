# Module 3: Reliable Reading & Dead Letter Queues (DLQs)

## Topic

Implementing reliable data ingestion using Dead Letter Queues (DLQs).

## Key Kafka Concepts

In Apache Kafka, brokers are fundamentally schema-agnostic, treating message keys and values as raw byte arrays. Consequently, payload parsing, schema validation, and fault tolerance mechanisms must be handled by the client application.

## Implementation Details

The `ReliableReadingStrategy` is implemented to ensure fault-tolerant processing. It utilizes immediate Dead Letter Queue (DLQ) routing to isolate "poison pill" records upon encountering parsing or validation failures.

## Architecture Overview

* **Error Isolation:** Exceptions related to parsing or validation are caught on a per-record basis, preventing the interruption of the entire partition consumption loop.
* **DLQ Forwarding:** Corrupted or "poison pill" payloads are automatically routed to a dedicated Dead Letter Queue (`users-topic.dlq`).
* **Header Enrichment:** Diagnostic context headers (e.g., `x-exception-message`, `x-original-topic`, `x-original-partition`, `x-original-offset`) are attached to the DLQ records for easier debugging and tracking.


## Code Snippet Focus

This code snippet demonstrates fault-tolerant reading and DLQ forwarding:

```java
public List<List<Object>> read(String tableName, int limit, Properties consumerProps, Map<String, TableSchema> tableToSchema, Map<String, Object> params) {
    // ...
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
            KafkaProducer<String, String> dlqProducer = new KafkaProducer<>(getProducerProps(consumerProps))) {
        // ...
        while (recordsFetched < limit) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    // 1. Per-record validation (throws exception if payload is malformed or corrupted)
                    validateRecord(record);

                    // 2. Parse valid CSV record (id, name, amount)
                    String[] fields = record.value().split(",");
                    validRecords.add(Arrays.asList(
                        fields[0].trim(), 
                        fields[1].trim(), 
                        Double.parseDouble(fields[2].trim())
                    ));
                } catch (Exception e) {
                    // 3. Error Isolation: Route poison pill to DLQ without failing consumer loop
                    sendToDlq(dlqProducer, dlqTopic, record, e);
                }
            }
        }
    }
    return validRecords;
}

/**
 * Strict payload validation check for required columns and poison pill keywords.
 */
private void validateRecord(ConsumerRecord<String, String> record) {
    String value = record.value();
    if (value == null) {
        throw new IllegalArgumentException("Null record payload detected");
    }
    if (value.contains("POISON_PILL") || value.contains("CORRUPTED")) {
        throw new IllegalArgumentException("Explicit poison pill payload detected: " + value);
    }
    String[] fields = value.split(",");
    if (fields.length < 3) {
        throw new IllegalArgumentException("Malformed payload (expected 3 CSV fields, got " + fields.length + "): " + value);
    }
    try {
        Double.parseDouble(fields[2].trim());
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid numeric value for column 'amount': " + fields[2]);
    }
}

/**
 * Attaches metadata context headers to poison pill records before sending to the DLQ.
 */
private void sendToDlq(KafkaProducer<String, String> producer, String dlqTopic, ConsumerRecord<String, String> record, Exception exception) {
    ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
            dlqTopic,
            null,
            record.key(),
            record.value()
    );

    // Enrich DLQ record with diagnostic headers
    dlqRecord.headers().add(new RecordHeader("x-exception-message", exception.getMessage().getBytes(StandardCharsets.UTF_8)));
    dlqRecord.headers().add(new RecordHeader("x-original-topic", record.topic().getBytes(StandardCharsets.UTF_8)));
    dlqRecord.headers().add(new RecordHeader("x-original-partition", String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8)));
    dlqRecord.headers().add(new RecordHeader("x-original-offset", String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8)));
    dlqRecord.headers().add(new RecordHeader("x-exception-timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));

    producer.send(dlqRecord);
    producer.flush();
}
```

## Best Practices

* Always validate records before processing them further to ensure data integrity.
* Direct bad records to the DLQ for later asynchronous investigation and recovery.

## Verification

* Run `scripts\seed_module3_dlg.py` to populate test data, including bad records.
* Run the application specifying `--module 0` and `--module 3`, and compare the results.
* Run the application with `--module 3 --read_dlq` to view the DLGs sent to the `users_topic_dlg` topic.
