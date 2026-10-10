package xsgao.kafkaconnector.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Core engine to read data from Kafka and convert it into a result set format.
 */
public class KafkaReader {
    private final Properties consumerProps;
    private final Map<String, TableSchema> tableToSchema;
    private ReadingStrategy strategy;

    public KafkaReader(String bootstrapServers, Map<String, TableSchema> metadata, ReadingStrategy strategy) {
        this.tableToSchema = new HashMap<>();
        for (TableSchema schema : metadata.values()) {
            this.tableToSchema.put(schema.getTableName(), schema);
        }

        this.strategy = strategy;
        this.consumerProps = new Properties();
        // Host/port pairs for initial cluster connection
        this.consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Unique ID for the consumer group
        this.consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-connector-group-" + UUID.randomUUID());
        // Deserializer for message keys
        this.consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Deserializer for message values
        this.consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Start from the beginning if no offset exists
        this.consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public void setStrategy(ReadingStrategy strategy) {
        this.strategy = strategy;
    }

    public List<List<Object>> read(String tableName, int limit, Map<String, Object> params) {
        return strategy.read(tableName, limit, consumerProps, tableToSchema, params);
    }
}
