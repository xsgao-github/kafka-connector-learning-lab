package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import xsgao.kafkaconnector.model.TableSchema;

public class GracefulShutdownStrategy extends AbstractReadingStrategy {

    @Override
    public List<List<Object>> read(String tableName, int limit, Properties consumerProps, Map<String, TableSchema> tableToSchema, Map<String, Object> params) {

        TableSchema tableSchema = tableToSchema.get(tableName);
        if (tableSchema == null) {
            throw new IllegalArgumentException("No schema metadata found for table: " + tableName);
        }

        String topicName = tableSchema.getTopicName();
        List<List<Object>> processedRecords = new ArrayList<>();

        // 1. STATIC MEMBERSHIP (group.instance.id & session.timeout.ms):
        // Prevents consumer group rebalance storms during pod restarts by holding partition assignments open.
        String instanceId = (params != null && params.containsKey("group.instance.id"))
                ? params.get("group.instance.id").toString()
                : "k8s-pod-node-1";

        consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        // Timeout range: 45s-5m. Broker holds partitions open for instanceId during container rollouts.
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        // 2. SIGNAL-DRIVEN SHUTDOWN HOOK:
        // Catches SIGTERM / Ctrl+C. Calls wakeup() (the ONLY thread-safe method on KafkaConsumer) to break poll().
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Shutdown Hook] SIGTERM detected. Triggering consumer.wakeup()...");
            consumer.wakeup();
            try {
                mainThread.join(); // Wait for main thread to finish final commit & cleanup
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(Collections.singletonList(topicName));

            int batchCount = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                batchCount++;

                // 1. Only process and commit if new records arrived
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        // Process record
                        record.key();
                        record.value();
                    }
                    consumer.commitSync();
                    
                    // Note the %n at the end to flush stdout immediately to terminal!
                    System.out.printf("[Module 9] Instance ID: %s | Processed %d records & committed offsets.%n", 
                            instanceId, records.count());
                } else if (batchCount % 5 == 0) {
                    // Optional: Print a status heartbeat every 5 seconds while waiting
                    System.out.printf("[Module 9] Instance ID: %s | Waiting for new records...%n", instanceId);
                }
            }

        } catch (WakeupException e) {
            System.out.println("[Module 9] Interrupted by wakeup(). Commencing clean exit...");
        } finally {
            // 3. FINAL OFFSET COMMIT & SINK FLUSH:
            // - Idempotent Sinks (UPSERT): commitSync() prevents re-reading historical topics from zero on pod restart.
            // - DB-Managed Atomic Offsets: commitSync() updates Kafka metrics so lag monitoring tools don't report 100% false lag.
            // Fast execution (<50ms network call) easily completes within K8s default grace period (30s) before SIGKILL.
            try {
                System.out.println("[Module 9] Flushing final in-flight records and committing offsets...");
                consumer.commitSync();
            } catch (Exception e) {
                System.err.println("[Module 9] Commit failed during shutdown: " + e.getMessage());
            } finally {
                consumer.close();
                System.out.println("[Module 9] Socket closed cleanly.");
            }
        }

        return processedRecords;
    }
}
