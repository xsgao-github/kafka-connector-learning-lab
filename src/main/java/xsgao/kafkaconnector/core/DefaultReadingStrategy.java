package xsgao.kafkaconnector.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Baseline consumption strategy (Module 0).
 * Uses Kafka's high-level 'subscribe' API.
 */
public class DefaultReadingStrategy extends AbstractReadingStrategy {
    @Override
    public List<List<Object>> read(String tableName, int limit, Properties consumerProps, Map<String, TableSchema> tableToSchema, Map<String, Object> params) {
        TableSchema targetSchema = tableToSchema.get(tableName);
        if (targetSchema == null) {
            throw new IllegalArgumentException("Table not found in metadata: " + tableName);
        }

        String topic = targetSchema.getTopicName();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            // Subscribe to the topic using Kafka's high-level consumer API
            consumer.subscribe(Collections.singletonList(topic));

            return pollRecords(consumer, targetSchema, limit);
        }
    }
}
