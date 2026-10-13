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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Module 7 Strategy: Observability (Consumer Lag & Metrics).
 * Simulates downstream sink processing delay to demonstrate lag accumulation
 * and triggers System.err alerts when unread message lag exceeds a threshold.
 */
public class ObservabilityStrategy extends AbstractReadingStrategy {

    private static final long LAG_THRESHOLD = 300; // Unread message threshold for alerts

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
        List<List<Object>> processedRecordsSummary = new ArrayList<>();
        long totalRecordsProcessed = 0;
        int batchNumber = 0;

        System.out.printf("[Module 7 Observability] Subscribing to '%s'... Press Ctrl+C to terminate.%n", topicName);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topicName));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                Set<TopicPartition> assignedPartitions = consumer.assignment();

                batchNumber++;
                int batchSize = records.count();
                totalRecordsProcessed += batchSize;

                // 1. Simulate downstream sink latency (e.g., writing to a DB or external API)
                if (batchSize > 0) {
                    if (!simulateSinkProcessing(batchSize)) {
                        System.err.println("[Module 7 Observability] Interrupted during simulated sink processing. Exiting...");
                        break;
                    }
                }

                // 2. Compute Lag across assigned partitions
                long totalLag = 0;
                long maxPartitionPosition = 0;
                long maxHighWatermark = 0;

                if (!assignedPartitions.isEmpty()) {
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignedPartitions);

                    for (TopicPartition tp : assignedPartitions) {
                        long currentPosition = consumer.position(tp);
                        long logEndOffset = endOffsets.getOrDefault(tp, currentPosition);
                        long partitionLag = Math.max(0, logEndOffset - currentPosition);

                        totalLag += partitionLag;
                        maxPartitionPosition = Math.max(maxPartitionPosition, currentPosition);
                        maxHighWatermark = Math.max(maxHighWatermark, logEndOffset);
                    }
                }

                double nativeMaxLag = getNativeMetricValue(consumer, "records-lag-max");

                // 3. Print regular batch progress
                if (batchSize > 0 || batchNumber % 5 == 0) {
                    System.out.printf(
                        "[Module 7] Batch #%-4d | Fetched: %-4d records | Total Read: %-6d | Position: %-6d | High Watermark: %-6d | Calculated Lag: %-5d | Native Max Lag: %.0f%n",
                        batchNumber,
                        batchSize,
                        totalRecordsProcessed,
                        maxPartitionPosition,
                        maxHighWatermark,
                        totalLag,
                        nativeMaxLag < 0 ? 0 : nativeMaxLag
                    );
                }

                // 4. Alert via System.err if unread message lag exceeds threshold
                if (totalLag > LAG_THRESHOLD) {
                    System.err.printf(
                        "[ALERT][Module 7] High Consumer Lag Detected! Unread messages: %d (Threshold: %d)%n",
                        totalLag, LAG_THRESHOLD
                    );
                }

                if (batchSize == 0) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        return processedRecordsSummary;
    }

    private double getNativeMetricValue(KafkaConsumer<String, String> consumer, String metricName) {
        Map<MetricName, ? extends Metric> metrics = consumer.metrics();
        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            if (entry.getKey().name().equals(metricName)) {
                Object val = entry.getValue().metricValue();
                if (val instanceof Double) {
                    return (Double) val;
                }
            }
        }
        return -1.0;
    }
}
