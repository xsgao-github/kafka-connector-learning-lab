package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Implementation of Module 1: Advanced Offset Consumption.
 * This strategy explores three different ways to position the read pointer in Kafka.
 */
public class OffsetReadingStrategy extends AbstractReadingStrategy {

    @Override
    public List<List<Object>> read(String tableName, int limit, Properties consumerProps, Map<String, TableSchema> tableToSchema, Map<String, Object> params) {
        TableSchema targetSchema = tableToSchema.get(tableName);
        if (targetSchema == null) {
            throw new IllegalArgumentException("Table not found in metadata: " + tableName);
        }
        String topic = targetSchema.getTopicName();
        long offset = (long) params.getOrDefault("offset", 0L);

        Properties props = new Properties();
        props.putAll(consumerProps);
        // Best Practice: Explicitly set enable.auto.commit = false.
        // WHY: When implementing custom offset seeking or manual offset tracking, background auto-commit 
        // threads can asynchronously commit un-processed offsets, corrupting or overwriting manual state.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        if (params.containsKey("timestamp")) {
            return readByTimestamp(topic, (Long) params.get("timestamp"), props, targetSchema, limit);
        } else if (params.containsKey("useRebalanceListener")) {
            return readByRebalanceListener(topic, offset, props, targetSchema, limit);
        } else {
            return readByManualSeek(topic, offset, props, targetSchema, limit);
        }
    }

    /**
     * EXPERIMENT 1: Manual Seek (Direct Control)
     * Uses 'assign' and 'seek' to jump to an absolute offset across all partitions.
     */
    private List<List<Object>> readByManualSeek(String topic, long offset, Properties props, TableSchema schema, int limit) {
        // Best Practice: Wrap KafkaConsumer in try-with to avoid socket leaks.
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = getAllPartitions(consumer, topic);
            consumer.assign(partitions);

            // Best Practice: Call consumer.poll(Duration.ofMillis(0)) immediately after assign().
            // WHY: Never call seek() right after assign() without polling first. The Kafka client requires 
            // an initial poll to discover cluster topology, initialize partition assignments, and establish socket coordinates.
            consumer.poll(Duration.ofMillis(0));

            for (TopicPartition tp : partitions) {
                consumer.seek(tp, offset);
            }
            System.out.println("Mode: Manual Seek | Offset: " + offset + " | Partitions: " + partitions.size());
            return pollRecords(consumer, schema, limit);
        }
    }

    /**
     * EXPERIMENT 2: Timestamp-Based Seeking (Time-Travel)
     * Translates a wall-clock timestamp into offsets using 'offsetsForTimes'.
     */
    private List<List<Object>> readByTimestamp(String topic, long timestamp, Properties props, TableSchema schema, int limit) {
        // Best Practice: Wrap KafkaConsumer in try-with to avoid socket leaks.
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = getAllPartitions(consumer, topic);
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition tp : partitions) {
                timestampsToSearch.put(tp, timestamp);
            }
            Map<TopicPartition, OffsetAndTimestamp> foundOffsets = consumer.offsetsForTimes(timestampsToSearch);
            consumer.assign(partitions);

            // Best Practice: Initial poll before seeking.
            // WHY: Ensures consumer group/partition state and socket connections are fully initialized prior to seeking.
            consumer.poll(Duration.ofMillis(0));

            for (TopicPartition tp : partitions) {
                OffsetAndTimestamp offsetAndTimestamp = foundOffsets.get(tp);
                if (offsetAndTimestamp != null) {
                    consumer.seek(tp, offsetAndTimestamp.offset());
                } else {
                    consumer.seekToEnd(Collections.singletonList(tp));
                }
            }
            System.out.println("Mode: Timestamp Seek | Timestamp: " + timestamp + " | Partitions: " + partitions.size());
            return pollRecords(consumer, schema, limit);
        }
    }

    /**
     * EXPERIMENT 3: Rebalance Listener (Group-Based Seek)
     * Combines 'subscribe' (Group Management) with custom seeking during the assignment phase.
     */
    private List<List<Object>> readByRebalanceListener(String topic, long offset, Properties props, TableSchema schema, int limit) {
        // Best Practice: Wrap KafkaConsumer in try-with to avoid socket leaks.
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Called when rebalance revokes assigned partitions
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("Rebalance Listener: Partitions assigned. Seeking to " + offset);
                    for (TopicPartition tp : partitions) {
                        // Executed safely inside the rebalance callback once partition assignments are established by broker
                        consumer.seek(tp, offset);
                    }
                }
            });
            System.out.println("Mode: Rebalance Listener | Offset: " + offset);
            return pollRecords(consumer, schema, limit);
        }
    }
}
