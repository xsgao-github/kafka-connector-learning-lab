package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Module 5: Delivery Semantics Strategy.
 * Demonstrates At-Most-Once, At-Least-Once, and Exactly-Once (EOS) delivery guarantees.
 */
public class ExactlyOnceReadingStrategy extends AbstractReadingStrategy {

    // External state store for EOS offsets (simulates DB table / checkpoint file)
    private static final Map<TopicPartition, Long> externalOffsetStore = new ConcurrentHashMap<>();

    @Override
    public List<List<Object>> read(String tableName, int limit, Properties consumerProps, Map<String, TableSchema> tableToSchema, Map<String, Object> params) {
        TableSchema targetSchema = tableToSchema.get(tableName);
        if (targetSchema == null) {
            throw new IllegalArgumentException("Table not found in metadata: " + tableName);
        }

        String topic = targetSchema.getTopicName();
        String mode = ((String) params.getOrDefault("mode", "exactly_once")).toLowerCase();
        boolean shouldCrash = (boolean) params.getOrDefault("crash", false) 
                           || params.containsKey("crash_before_processing")
                           || ((int) params.getOrDefault("crash_after", -1) > 0);

        // Build mode-isolated Consumer Group ID to prevent offset cross-contamination in Kafka
        Properties props = new Properties();
        props.putAll(consumerProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-connector-group-" + mode);

        switch (mode) {
            case "at_most_once":
                return readAtMostOnce(topic, limit, props, shouldCrash);
            case "at_least_once":
                return readAtLeastOnce(topic, limit, props, shouldCrash);
            case "exactly_once":
            case "eos":
            default:
                return readExactlyOnce(topic, limit, props, shouldCrash);
        }
    }

    /**
     * 1. At-Most-Once Mode
     * Commits offsets immediately after poll() before processing.
     * If a crash occurs before return, remaining polled records are permanently lost on Kafka.
     */
    private List<List<Object>> readAtMostOnce(String topic, int limit, Properties consumerProps, boolean shouldCrash) {
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<List<Object>> results = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = getAllPartitions(consumerProps, topic);
            consumer.assign(partitions);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            // AT-MOST-ONCE KEY STEP: Commit offset IMMEDIATELY after poll before processing payload!
            if (!records.isEmpty()) {
                consumer.commitSync();
                System.out.println("[At-Most-Once] Committed offsets to Kafka immediately after poll.");
            }

            for (ConsumerRecord<String, String> record : records) {
                if (record.value() == null) continue;

                List<Object> row = parseRecord(record);
                if (!row.isEmpty()) results.add(row);

                if (results.size() >= limit) break;
            }

            System.out.println("The following records were fetched: " + results.stream().map(r -> r.get(0)).collect(Collectors.toList()));

            // Simulated crash before returning results
            if (shouldCrash) {
                System.err.println("🔥 [CRASH] Client crashed after record " + results.size());
                throw new RuntimeException("Simulated mid-batch failure in At-Most-Once mode");
            }
        }

        return results;
    }

    /**
     * 2. At-Least-Once Mode
     * Disables auto-commit and explicitly calls commitSync() ONLY AFTER batch processing completes.
     * If a crash occurs before commitSync(), the next run re-fetches and duplicates records.
     */
    private List<List<Object>> readAtLeastOnce(String topic, int limit, Properties consumerProps, boolean shouldCrash) {
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<List<Object>> results = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = getAllPartitions(consumerProps, topic);
            consumer.assign(partitions);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                if (record.value() == null) continue;

                List<Object> row = parseRecord(record);
                if (!row.isEmpty()) results.add(row);

                if (results.size() >= limit) break;
            }

            System.out.println("The following records were fetched: " + results.stream().map(r -> r.get(0)).collect(Collectors.toList()));

            // Simulated crash BEFORE calling consumer.commitSync()
            if (shouldCrash) {
                System.err.println("🔥 [CRASH] Client crashed after record " + results.size() + " before commitSync()");
                throw new RuntimeException("Simulated mid-batch failure in At-Least-Once mode");
            }

            // Explicit manual commit strictly AFTER batch processing succeeds
            consumer.commitSync();
            System.out.println("[At-Least-Once] Explicitly committed offsets to Kafka.");
        }

        return results;
    }

    /**
     * 3. Exactly-Once (EOS) Mode
     * Positions consumer via seek() using external offset store, enforces idempotency,
     * and updates external offset checkpoint store alongside row processing.
     */
    private List<List<Object>> readExactlyOnce(String topic, int limit, Properties consumerProps, boolean shouldCrash) {
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<List<Object>> results = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = getAllPartitions(consumerProps, topic);
            consumer.assign(partitions);

            // Position consumer to (last_checkpoint + 1) via seek()
            for (TopicPartition tp : partitions) {
                if (externalOffsetStore.containsKey(tp)) {
                    long nextOffset = externalOffsetStore.get(tp) + 1;
                    consumer.seek(tp, nextOffset);
                    System.out.println("[EOS] Seeking partition " + tp.partition() + " to offset " + nextOffset);
                }
            }

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                if (record.value() == null) continue;

                TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                // Idempotency Guard: skip records already checkpointed
                if (externalOffsetStore.containsKey(tp) && record.offset() <= externalOffsetStore.get(tp)) {
                    System.out.println("[EOS] Skipping duplicate offset: " + record.offset());
                    continue;
                }

                List<Object> row = parseRecord(record);
                if (!row.isEmpty()) results.add(row);

                // Atomically update external state checkpoint with row processing
                externalOffsetStore.put(tp, record.offset());

                if (results.size() >= limit) break;
            }

            System.out.println("The following records were fetched: " + results.stream().map(r -> r.get(0)).collect(Collectors.toList()));

            // Simulated crash before return (checkpoint is already saved in externalOffsetStore)
            if (shouldCrash) {
                System.err.println("🔥 [CRASH] Client crashed after record " + results.size());
                throw new RuntimeException("Simulated mid-batch failure in EOS mode");
            }
        }

        return results;
    }

    private List<Object> parseRecord(ConsumerRecord<String, String> record) {
        String[] fields = record.value().split(",");
        if (fields.length < 3) return Collections.emptyList();

        return Arrays.asList(
            fields[0].trim(),                     // id
            fields[1].trim(),                     // name
            Double.parseDouble(fields[2].trim()), // balance
            record.partition(),                   // partition_id
            record.offset()                       // offset_id
        );
    }

    public static Map<TopicPartition, Long> getExternalOffsetStore() {
        return externalOffsetStore;
    }
}
