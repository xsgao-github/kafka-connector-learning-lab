package xsgao.kafkaconnector.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;

import xsgao.kafkaconnector.model.TableSchema;

/**
 * Module 6 Strategy: Schema Discovery & Evolution.
 * Dynamically resolves payload version using Kafka headers, matches input against
 * versioned metadata, and projects rows into the target table schema (widest version).
 */
public class SchemaEvolutionStrategy extends AbstractReadingStrategy {

    private static final String DEFAULT_VERSION = "v1";

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
        List<List<Object>> results = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topicName));

            int emptyPolls = 0;
            while (results.size() < limit && emptyPolls < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;

                for (ConsumerRecord<String, String> record : records) {
                    if (results.size() >= limit) {
                        break;
                    }

                    List<Object> normalizedRow = processRecord(record, tableSchema);
                    if (normalizedRow != null) {
                        results.add(normalizedRow);
                    }
                }
            }
        }

        return results;
    }

    private List<Object> processRecord(ConsumerRecord<String, String> record, TableSchema tableSchema) {
        if (record.value() == null) {
            return null;
        }

        // 1. Extract schema_version from Kafka headers ("v1", "v2", "v3", etc.)
        String schemaVersion = extractSchemaVersion(record);

        // 2. Resolve input column definition for this specific version
        List<TableSchema.ColumnDefinition> versionColumns = null;
        if (tableSchema.getVersions() != null) {
            versionColumns = tableSchema.getVersions().get(schemaVersion);
        }

        // Fallback to top-level columns if version-specific columns are missing
        if (versionColumns == null) {
            versionColumns = tableSchema.getColumns();
        }

        // 3. Resolve destination target schema (picks the widest schema between top-level and versions)
        List<TableSchema.ColumnDefinition> targetColumns = resolveTargetColumns(tableSchema);

        // 4. Parse CSV payload
        String[] fields = record.value().split(",");

        // 5. Map & normalize to target table columns
        List<Object> normalizedRow = new ArrayList<>();

        for (int i = 0; i < targetColumns.size(); i++) {
            if (i < fields.length && i < versionColumns.size()) {
                normalizedRow.add(parseFieldValue(fields[i].trim(), targetColumns.get(i)));
            } else {
                // Pad missing fields (e.g., 'email' and 'phone' in v1 records) with null
                normalizedRow.add(null);
            }
        }

        return normalizedRow;
    }

    /**
     * Resolves target table schema by picking the widest column list across
     * top-level columns and all version definitions.
     */
    private List<TableSchema.ColumnDefinition> resolveTargetColumns(TableSchema tableSchema) {
        List<TableSchema.ColumnDefinition> target = tableSchema.getColumns();
        if (tableSchema.getVersions() != null) {
            for (List<TableSchema.ColumnDefinition> cols : tableSchema.getVersions().values()) {
                if (target == null || cols.size() > target.size()) {
                    target = cols;
                }
            }
        }
        return target;
    }

    private String extractSchemaVersion(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("schema_version");
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return DEFAULT_VERSION;
    }

    private Object parseFieldValue(String rawValue, TableSchema.ColumnDefinition colDef) {
        if (rawValue == null || rawValue.isEmpty()) {
            return null;
        }
        if (colDef.getType() != null) {
            switch (colDef.getType()) {
                case INT:
                    return Integer.parseInt(rawValue);
                case DOUBLE:
                    return Double.parseDouble(rawValue);
                default:
                    return rawValue;
            }
        }
        return rawValue;
    }
}