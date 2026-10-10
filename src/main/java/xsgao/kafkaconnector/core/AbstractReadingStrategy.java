package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import xsgao.kafkaconnector.model.ColumnType;
import xsgao.kafkaconnector.model.TableSchema;

/**
 * Base class for reading strategies to provide shared parsing and casting utilities.
 */
public abstract class AbstractReadingStrategy implements ReadingStrategy {


    /**
     * Get partition resolution using an existing consumer instance.
     * Best Practice: Partition resolution rarely changes, so caching partition info with a long 
     * TTL (time-to-live) can improve performance.
     */
    protected List<TopicPartition> getAllPartitions(KafkaConsumer<?, ?> consumer, String topic) {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            throw new IllegalStateException("Could not find partitions for topic: " + topic);
        }
        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo info : partitionInfos) {
            partitions.add(new TopicPartition(topic, info.partition()));
        }
        return partitions;
    }

    /**
     * Overloaded helper to safely query cluster metadata via a temporary consumer.
     */
    protected List<TopicPartition> getAllPartitions(Properties props, String topic) {
        // Best Practice: Encloses the temporary client in try-with-resources to prevent socket leaks.
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            return getAllPartitions(consumer, topic);
        }
    }
    
    /**
     * Generic polling loop to retrieve and convert records from a Kafka consumer.
     */
    protected List<List<Object>> pollRecords(KafkaConsumer<String, String> consumer, TableSchema targetSchema, int limit) {
        List<List<Object>> results = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        while (results.size() < limit) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                // Manual assignment or rebalancing may take time; allow 15s timeout
                if (System.currentTimeMillis() - startTime > 15000) break;
                continue;
            }

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                if (value == null) continue;
                String[] lines = value.split("\\r?\\n");
                for (String line : lines) {
                    if (line.trim().isEmpty()) continue;
                    results.add(convertMessageToRow(line, targetSchema));
                    if (results.size() >= limit) break;
                }
                if (results.size() >= limit) break;
            }
        }
        return results;
    }

    protected List<Object> convertMessageToRow(String message, TableSchema schema) {
        if (message == null) return new ArrayList<>();

        String[] values = message.split("[,\\s]+");

        if (values.length != schema.getColumns().size()) {
            System.err.println("WARNING: Malformed record detected. Expected "
                + schema.getColumns().size() + " columns, found " + values.length
                + ". Record: [" + message + "]");
        }

        List<Object> row = new ArrayList<>();
        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (i >= values.length) {
                row.add(null);
                continue;
            }

            String val = values[i].trim();
            TableSchema.ColumnDefinition col = schema.getColumns().get(i);
            row.add(castToType(val, col.getType()));
        }
        return row;
    }

    protected Object castToType(String value, ColumnType type) {
        if (value == null || value.isEmpty()) return null;
        try {
            return switch (type) {
                case INT -> Integer.valueOf(value);
                case DOUBLE -> Double.valueOf(value);
                case BOOLEAN -> Boolean.valueOf(value);
                case STRING -> value;
            };
        } catch (NumberFormatException e) {
            System.err.println("ERROR: Failed to cast value [" + value + "] to type " + type);
            return null;
        }
    }
}
