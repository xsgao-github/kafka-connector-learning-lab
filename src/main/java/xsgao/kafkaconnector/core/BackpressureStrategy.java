package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Module 8 Strategy: Flow Control & Backpressure (pause() / resume()).
 * Demonstrates keeping consumer.poll() running for liveness while using
 * pause() and resume() to prevent buffer overflows during slow downstream processing.
 */
public class BackpressureStrategy extends AbstractReadingStrategy {

    private static final int HIGH_WATERMARK_BUFFER = 1000; // Pause threshold
    private static final int LOW_WATERMARK_BUFFER = 200;   // Resume threshold

    @Override
    public List<List<Object>> read(
            String tableName,
            int limit,
            Properties consumerProps,
            Map<String, TableSchema> tableToSchema,
            Map<String, Object> params) {

        TableSchema tableSchema = tableToSchema.get(tableName);
        if (tableSchema == null) {
            throw new IllegalArgumentException("No schema metadata found for table: " + tableName);
        }

        String topicName = tableSchema.getTopicName();
        List<List<Object>> processedRecords = new ArrayList<>();
        int currentBufferBacklog = 0;
        boolean isPaused = false;
        int batchNumber = 0;

        System.out.printf("[Module 8 Backpressure] Subscribing to '%s'... Press Ctrl+C to stop.%n", topicName);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topicName));

            while (true) {
                // 1. ALWAYS call poll() to maintain group heartbeats & prevent max.poll.interval.ms timeouts
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                Set<TopicPartition> assignedPartitions = consumer.assignment();

                batchNumber++;
                int fetchedCount = records.count();
                currentBufferBacklog += fetchedCount;

                // 2. Simulate downstream processing draining the buffer
                if (currentBufferBacklog > 0) {
                    int drainedThisBatch = Math.min(currentBufferBacklog, 200);
                    if (!simulateSinkProcessing(records.count())) {
                        break;
                    }
                    currentBufferBacklog -= drainedThisBatch;
                }

                // 3. Apply High Watermark Backpressure (Pause fetching)
                if (!isPaused && currentBufferBacklog >= HIGH_WATERMARK_BUFFER && !assignedPartitions.isEmpty()) {
                    consumer.pause(assignedPartitions);
                    isPaused = true;
                    System.err.printf("[BACKPRESSURE ALERT] Buffer backlog (%d >= %d). PAUSING partitions: %s%n",
                            currentBufferBacklog, HIGH_WATERMARK_BUFFER, assignedPartitions);
                }

                // 4. Apply Low Watermark Recovery (Resume fetching)
                if (isPaused && currentBufferBacklog <= LOW_WATERMARK_BUFFER && !assignedPartitions.isEmpty()) {
                    consumer.resume(assignedPartitions);
                    isPaused = false;
                    System.out.printf("[BACKPRESSURE RESUME] Buffer drained (%d <= %d). RESUMING partitions: %s%n",
                            currentBufferBacklog, LOW_WATERMARK_BUFFER, assignedPartitions);
                }

                // 5. Log batch status line
                if (fetchedCount > 0 || batchNumber % 5 == 0 || isPaused) {
                    System.out.printf("[Module 8] Batch #%-4d | Fetched: %-4d records | Buffer Backlog: %-5d | Paused: %b%n",
                            batchNumber, fetchedCount, currentBufferBacklog, isPaused);
                }
            }
        }

        return processedRecords;
    }
}
