package xsgao.kafkaconnector.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Module 3: Reliability & Dead Letter Queue (DLQ) Strategy.
 * Implements fault-tolerant processing with immediate Dead Letter Queue (DLQ) poison pill routing upon parsing failure.
 */
public class ReliableReadingStrategy extends AbstractReadingStrategy {

    @Override
    public List<List<Object>> read(String tableName, int limit, Properties consumerProps, Map<String, TableSchema> tableToSchema, Map<String, Object> params) {
        // Check if the caller explicitly requested reading from the DLQ
        boolean readDlq = (boolean) params.getOrDefault("read_dlq", false);
        if (readDlq) {
            return readDlq(tableName, consumerProps, tableToSchema);
        }

        TableSchema targetSchema = tableToSchema.get(tableName);
        if (targetSchema == null) {
            throw new IllegalArgumentException("Table not found in metadata: " + tableName);
        }
        String topic = targetSchema.getTopicName();
        String dlqTopic = (String) params.getOrDefault("dlqTopic", topic + "_dlq");

        // Best Practice: Explicitly set enable.auto.commit = false.
        // WHY: Manual offset tracking and DLQ poison pill handling require explicit control over offset advancement.
        Properties props = new Properties();
        props.putAll(consumerProps);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Best Practice: Configure DLQ Producer properties for guaranteed durability.
        // WHY: Setting acks=all guarantees that dead-letter records are durably written before advancing consumer state.
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        List<List<Object>> validRecords = new ArrayList<>();

        // Best Practice: Wrap Consumer and Producer in try-with-resources.
        // WHY: Ensures both socket connections and producer buffers are flushed and closed properly upon completion.
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             KafkaProducer<String, String> dlqProducer = new KafkaProducer<>(producerProps)) {

            List<TopicPartition> partitions = getAllPartitions(consumer, topic);
            consumer.assign(partitions);

            // Best Practice: Call consumer.poll(Duration.ofMillis(0)) immediately after assign().
            // WHY: Establishes cluster topology, partition assignment, and broker socket coordinates before seeking or processing records.
            consumer.poll(Duration.ofMillis(0));

            if (params.containsKey("offset")) {
                long offset = (long) params.get("offset");
                for (TopicPartition tp : partitions) {
                    consumer.seek(tp, offset);
                }
            }

            int recordsFetched = 0;
            boolean keepPolling = true;

            while (keepPolling && recordsFetched < limit) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Inline Local Parsing: Convert raw record payload into structured row
                        String value = record.value();

                        if (value == null) {
                            throw new IllegalArgumentException("Null record payload");
                        }

                        String[] fields = value.split(",");

                        // 1. Check required column count (id, name, amount)
                        if (fields.length < 3) {
                            throw new IllegalArgumentException("Malformed payload (missing fields): " + value);
                        }

                        // 2. Validate data types (e.g., id and balance)
                        try {
                            Integer.parseInt(fields[0].trim());
                            Double.parseDouble(fields[2].trim());
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid ID field in payload: " + fields);
                        }

                        // 3. Check for explicit poison pill triggers
                        if (value.contains("POISON_PILL")) {
                            throw new IllegalArgumentException("Explicit poison pill detected: " + value);
                        }

                        // If all checks pass, build valid row
                        List<Object> row = List.of(fields[0].trim(), fields[1].trim(), fields[2].trim());
                        validRecords.add(row);
                    } catch (Exception e) {
                        System.err.printf("Poison pill detected on [Topic: %s, Partition: %d, Offset: %d]: %s%n",
                                record.topic(), record.partition(), record.offset(), e.getMessage());

                        // Best Practice: Poison Pill Routing to Dead Letter Queue (DLQ).
                        // WHY: When local record parsing fails, immediately route the message to a DLQ topic with diagnostic headers
                        // so processing continues without getting stuck in an infinite loop or crashing the pipeline.
                        sendToDlq(dlqProducer, dlqTopic, record, e);
                    }
                }
            }
        }

        return validRecords;
    }

    /**
     * Publishes failed records to the Dead Letter Queue decorated with diagnostic metadata headers.
     */
    private void sendToDlq(KafkaProducer<String, String> dlqProducer, String dlqTopic, ConsumerRecord<String, String> record, Exception exception) {
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("x-original-topic", record.topic().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("x-original-partition", String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("x-original-offset", String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("x-failure-timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));

        if (exception != null) {
            headers.add(new RecordHeader("x-exception-message",
                    (exception.getMessage() != null ? exception.getMessage() : exception.toString()).getBytes(StandardCharsets.UTF_8)));
        }

        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
                dlqTopic,
                null,
                record.timestamp(),
                record.key(),
                record.value(),
                headers
        );

        // Best Practice: Synchronous get() call on DLQ publishing.
        // WHY: Ensures the poison record is safely acknowledged by Kafka on the DLQ topic before the consumer pipeline moves forward.
        try {
            dlqProducer.send(dlqRecord).get();
            System.out.printf("Routed poison pill to DLQ topic [%s] | Original Offset: %d%n", dlqTopic, record.offset());
        } catch (Exception e) {
            System.err.printf("Failed to publish record to DLQ topic [%s]: %s%n", dlqTopic, e.getMessage());
        }
    }

    /**
     * Reads and inspects dead-lettered messages from the DLQ topic along with error headers.
     */
    private List<List<Object>> readDlq(String tableName, Properties consumerProps, Map<String, TableSchema> tableToSchema) {
        TableSchema targetSchema = tableToSchema.get(tableName);
        if (targetSchema == null) {
            throw new IllegalArgumentException("Table not found in metadata: " + tableName);
        }
        
        // DLQ topic naming convention: <topic_name>_dlq
        String dlqTopic = targetSchema.getTopicName() + "_dlq";

        Properties props = new Properties();
        props.putAll(consumerProps);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<List<Object>> dlqRecords = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = getAllPartitions(props, dlqTopic);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions); // Read DLQ from the beginning

            int emptyPolls = 0;
            final int maxEmptyPolls = 3;

            while (emptyPolls < maxEmptyPolls) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;

                for (ConsumerRecord<String, String> record : records) {
                    List<Object> row = new ArrayList<>();
                    row.add(record.key());
                    row.add(record.value());

                    // Extract diagnostic DLQ headers (exception message, original offset, timestamp)
                    Map<String, String> headerMap = new HashMap<>();
                    for (Header header : record.headers()) {
                        headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                    }
                    row.add(headerMap);

                    dlqRecords.add(row);
                }
            }
        }

        return dlqRecords;
    }
}
