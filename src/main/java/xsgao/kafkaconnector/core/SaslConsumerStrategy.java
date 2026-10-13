package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Module 11 Strategy: SASL-Authenticated Consumer Strategy.
 * <p>
 * Key Concepts & Architecture:
 * 1. Programmatic JAAS Configuration: Injects SASL_PLAINTEXT properties 
 *    (security.protocol, sasl.mechanism, sasl.jaas.config) dynamically into the
 *    KafkaConsumer Properties map without requiring external client login files.
 * 2. Dual-Listener Integration: Routes authenticated requests to the SASL broker listener 
 *    on port 9093 (with credentials like 'alice'/'alice-secret'), preserving unauthenticated
 *    access on port 9092 for Modules 1–11.
 */
public class SaslConsumerStrategy extends AbstractReadingStrategy {

    @Override
    public List<List<Object>> read(
            String tableName,
            int limit,
            Properties consumerProps,
            Map<String, TableSchema> tableToSchema,
            Map<String, Object> params) {

        // 1. Resolve 'users' table name to 'users_topic' from metadata
        String topicName = tableName;
        if (tableToSchema != null && tableToSchema.containsKey(tableName)) {
            TableSchema schema = tableToSchema.get(tableName);
            if (schema.getTopicName() != null) {
                topicName = schema.getTopicName();
            }
        }

        Properties props = (Properties) consumerProps.clone();

        // Use the bootstrap address supplied by the CLI. Host clients use localhost:9003;
        // containerized clients can supply kafka:9093 when that listener is advertised internally.
        String bootstrapServers = props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("A Kafka bootstrap server is required for Module 11");
        }

        // 3. Inject SASL/PLAIN Authentication Settings
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"alice\" password=\"alice-secret\";");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sasl-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<List<Object>> results = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            System.out.printf("[Module 11] Subscribed to topic '%s' on %s over SASL_PLAINTEXT as user 'alice'...%n", topicName, bootstrapServers);

            long deadline = System.currentTimeMillis() + 10000;
            while (results.size() < limit && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[SASL Record] Key: %s | Value: %s | Partition: %d | Offset: %d%n",
                            record.key(), record.value(), record.partition(), record.offset());

                    List<Object> row = new ArrayList<>();
                    row.add(record.topic());
                    row.add(record.key());
                    row.add(record.value());
                    results.add(row);
                }
            }
        }

        return results;
    }
}
