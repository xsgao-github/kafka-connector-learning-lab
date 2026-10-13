package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Module 10 Strategy: Per-Topic Pre-Allocated Consumer Pool & Worker Execution Strategy.
 * <p>
 * <b>Architectural Overview:</b>
 * This reading strategy decouples topic consumer management from client record handling
 * using a multi-tiered pooling model:
 * <ul>
 *   <li><b>Per-Topic Consumer Pools:</b> Pre-allocates a dedicated {@link BlockingQueue} of pre-subscribed
 *       {@link KafkaConsumer} instances for each discovered topic (e.g., {@code users_topic}, {@code orders_topic}).
 *       Isolating consumers by topic prevents dynamic topic-switching rebalances and consumer instance churn.</li>
 *   <li><b>Pre-Allocated Worker Pool:</b> Executes polling tasks asynchronously across worker threads,
 *       drawing and returning consumers to their respective topic pools during task execution.</li>
 *   <li><b>Client Delegation Pattern:</b> Separates framework polling infrastructure from record processing logic
 *       by delegating raw {@link ConsumerRecords} directly to a client {@link Consumer} callback.</li>
 * </ul>
 * <p>
 * <b>Lifecycle & Execution Flow:</b>
 * <ol>
 *   <li><b>Initialization:</b> Pre-warms consumer object pools per topic using schema metadata from {@code metadata.json}.</li>
 *   <li><b>Acquisition:</b> A worker thread acquires an active consumer from the topic's pool.</li>
 *   <li><b>Polling & Processing:</b> The worker thread polls Kafka and passes raw batches to the client callback.</li>
 *   <li><b>Return & Cleanup:</b> Upon completion or interruption, offsets are committed synchronously and the consumer
 *       instance is safely returned to its topic pool for reuse.</li>
 * </ol>
 */
public class DedicatedTopicConsumerStrategy extends AbstractReadingStrategy {

    private static final int POOL_SIZE_PER_TOPIC = 2;
    private static final int WORKER_POOL_SIZE = 4;

    // Dedicated consumer object pools per topic
    private final Map<String, BlockingQueue<KafkaConsumer<String, String>>> topicConsumerPools = new ConcurrentHashMap<>();

    @Override
    public List<List<Object>> read(String tableName, int limit, Properties consumerProps, Map<String, TableSchema> tableToSchema, Map<String, Object> params) {

        List<String> targetTopics = getTargetTopics(tableToSchema);
        List<List<Object>> processedRecords = Collections.synchronizedList(new ArrayList<>());

        // Client Handler callback: client handles DB writes once records are polled
        Consumer<ConsumerRecords<String, String>> clientHandler = records -> {
            for (ConsumerRecord<String, String> record : records) {
                List<Object> row = new ArrayList<>();
                row.add(record.topic());
                row.add(record.key());
                row.add(record.value());
                processedRecords.add(row);
            }
        };

        // Step 1: Pre-allocate Consumer Pools for each topic
        for (String topic : targetTopics) {
            BlockingQueue<KafkaConsumer<String, String>> pool = new ArrayBlockingQueue<>(POOL_SIZE_PER_TOPIC);
            for (int i = 0; i < POOL_SIZE_PER_TOPIC; i++) {
                Properties props = (Properties) consumerProps.clone();
                props.put("client.id", "pool-consumer-" + topic + "-" + (i + 1));
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(topic));
                pool.add(consumer); // Put in topic pool
            }
            topicConsumerPools.put(topic, pool);
        }

        // Step 2: Pre-allocate Worker Pool
        ExecutorService workerPool = Executors.newFixedThreadPool(WORKER_POOL_SIZE);

        // Step 3: For each topic, pick worker & consumer, run, then return consumer to pool
        for (String topic : targetTopics) {
            workerPool.submit(() -> executeTopicPollingTask(topic, clientHandler));
        }

        workerPool.shutdown();
        try {
            workerPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeAllPools();
        }

        return processedRecords;
    }

    /**
     * Executes polling task for a specific topic using an acquired consumer from the topic's pool.
     */
    private void executeTopicPollingTask(String topic, Consumer<ConsumerRecords<String, String>> clientHandler) {

        BlockingQueue<KafkaConsumer<String, String>> pool = topicConsumerPools.get(topic);
        if (pool == null) {
            System.err.printf("[%s] No consumer pool found! Skipping execution.%n", topic);
            return;
        }

        KafkaConsumer<String, String> consumer = null;
        try {
            // 1. Acquire consumer instance from topic pool
            System.out.printf("[%s] Worker thread acquiring consumer from pool...%n", topic);
            consumer = pool.poll(5, TimeUnit.SECONDS);

            if (consumer == null) {
                System.err.printf("[%s] Timeout waiting to acquire consumer from pool!%n", topic);
                return;
            }

            System.out.printf("[%s] Consumer successfully acquired from pool. Starting polling loop...%n", topic);

            int batchCount = 0;
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                batchCount++;

                // 2. Log polled records and forward to client handler
                if (!records.isEmpty()) {
                    System.out.printf("[%s] Polled %d records (Batch #%d). Forwarding to client handler...%n",
                            topic, records.count(), batchCount);

                    clientHandler.accept(records);

                    consumer.commitAsync((offsets, exception) -> {
                        if (exception != null) {
                            System.err.printf("[%s] Async commit failed: %s%n", topic, exception.getMessage());
                        }
                    });
                } else if (batchCount % 10 == 0) {
                    // Heartbeat log while waiting for incoming stream records
                    System.out.printf("[%s] Waiting for new records... (Batch #%d)%n", topic, batchCount);
                }
            }

        } catch (WakeupException e) {
            System.out.printf("[%s] WakeupException caught during shutdown.%n", topic);
        } catch (InterruptedException e) {
            System.out.printf("[%s] Worker thread interrupted.%n", topic);
            Thread.currentThread().interrupt();
        } finally {
            if (consumer != null) {
                try {
                    // 3. Final synchronous commit before returning consumer to pool
                    System.out.printf("[%s] Executing final synchronous commitSync()...%n", topic);
                    consumer.commitSync();
                } catch (Exception e) {
                    System.err.printf("[%s] Final commitSync error: %s%n", topic, e.getMessage());
                } finally {
                    // 4. Return consumer instance back to topic pool
                    System.out.printf("[%s] Returning consumer instance back to topic pool.%n", topic);
                    pool.offer(consumer);
                }
            }
        }
    }

    private List<String> getTargetTopics(Map<String, TableSchema> tableToSchema) {
        List<String> topics = new ArrayList<>();
        if (tableToSchema != null) {
            for (TableSchema schema : tableToSchema.values()) {
                if (schema.getTopicName() != null && !topics.contains(schema.getTopicName())) {
                    topics.add(schema.getTopicName());
                }
            }
        }
        return topics;
    }

    private void closeAllPools() {
        for (BlockingQueue<KafkaConsumer<String, String>> pool : topicConsumerPools.values()) {
            for (KafkaConsumer<String, String> consumer : pool) {
                try { consumer.close(); } catch (Exception ignored) {}
            }
        }
    }
}
