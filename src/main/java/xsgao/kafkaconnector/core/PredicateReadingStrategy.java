package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Module 4: Predicate Pushdown Reading Strategy.
 * 
 * KEY ARCHITECTURAL TAKEAWAY:
 * Kafka brokers are schema-agnostic and store keys/values as raw byte arrays.
 * They cannot evaluate field expressions (e.g., WHERE amount > 50.0).
 * 
 * In a streaming connector, "Predicate Pushdown" means pushing filter evaluation 
 * down to the lowest client level—inside the poll() iteration loop—to discard
 * non-matching records before allocating downstream row objects.
 */
public class PredicateReadingStrategy extends AbstractReadingStrategy {

    @Override
    public List<List<Object>> read(String tableName, int limit, Properties consumerProps, Map<String, TableSchema> tableToSchema, Map<String, Object> params) {
        TableSchema targetSchema = tableToSchema.get(tableName);
        if (targetSchema == null) {
            throw new IllegalArgumentException("Table not found in metadata: " + tableName);
        }

        String topic = targetSchema.getTopicName();

        // 1. Extract Predicate Filter Parameters
        String[] filterParts = parseAndSetFilter((String) params.get("filter"));
        if (filterParts == null || filterParts.length != 3) {
            throw new IllegalArgumentException("Filter expression must be in the format: 'balance <op> <value>'");
        }
        String operator = filterParts[1];
        double predicateValue = Double.parseDouble(filterParts[2]);

        Properties props = new Properties();
        props.putAll(consumerProps);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<List<Object>> filteredRecords = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = getAllPartitions(props, topic);
            consumer.assign(partitions);
            consumer.poll(Duration.ofMillis(0));

            int recordsFetched = 0;
            boolean keepPolling = true;

            while (keepPolling && recordsFetched < limit) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() == null) continue;

                    String[] fields = record.value().split(",");
                    if (fields.length < 3) continue;

                    String id = fields[0].trim();
                    String name = fields[1].trim();
                    double amount = Double.parseDouble(fields[2].trim());

                    // LESSON: Evaluate predicate BEFORE creating downstream row objects or collections.
                    // Discarding non-matching records here eliminates unnecessary memory allocations.
                    if (evaluatePredicate(amount, operator, predicateValue)) {
                        filteredRecords.add(Arrays.asList(id, name, amount));

                        recordsFetched++;
                        if (recordsFetched >= limit) {
                            keepPolling = false;
                            break;
                        }
                    }
                }
            }
        }

        return filteredRecords;
    }

    /**
     * Simplified parser specifically for balance filter expressions (e.g., "balance > 50.0").
     */
    private static String[] parseAndSetFilter(String filterExpr) {
        if (filterExpr == null || filterExpr.trim().isEmpty()) {
            return null;
        }

        // Split "balance > 50.0" by whitespace -> ["balance", ">", "50.0"]
        String[] parts = filterExpr.trim().split("\\s+");

        if (parts.length == 3) {
            return parts;
        } else {
            throw new IllegalArgumentException("Expected format: 'balance <op> <value>' (e.g., 'balance > 50.0')");
        }
    }
    /**
     * Evaluates predicate logic at the transport layer.
     * Simulates pushing query engine filters down to the streaming reader.
     */
    private boolean evaluatePredicate(double val, String operator, double targetVal) {
        switch (operator) {
            case ">" -> {
                return val > targetVal;
            }
            case ">=" -> {
                return val >= targetVal;
            }
            case "<" -> {
                return val < targetVal;
            }
            case "<=" -> {
                return val <= targetVal;
            }
            case "==", "=" -> {
                return val == targetVal;
            }
            case "!=" -> {
                return val != targetVal;
            }
            default -> throw new IllegalArgumentException("Unsupported predicate operator: " + operator);
        }
    }

}
