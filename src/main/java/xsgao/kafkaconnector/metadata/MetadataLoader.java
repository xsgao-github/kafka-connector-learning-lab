package xsgao.kafkaconnector.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import xsgao.kafkaconnector.model.TableSchema;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads metadata from an external JSON file mapping Kafka topics to tables and vice-versa.
 * Supports schema versioning (v1, v2, etc.) for schema evolution.
 */
public class MetadataLoader {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, TableSchema> loadMetadata(String filePath) throws IOException {
        File file = new File(filePath);
        List<TableSchema> schemas = objectMapper.readValue(file, new TypeReference<List<TableSchema>>() {});

        Map<String, TableSchema> metadataMap = new HashMap<>();
        for (TableSchema schema : schemas) {
            // Map by tableName (e.g. "users") so strategy lookups like tableToSchema.get("users") work
            if (schema.getTableName() != null) {
                metadataMap.put(schema.getTableName(), schema);
            }
            // Also map by topicName (e.g. "users-topic") as a fallback key
            if (schema.getTopicName() != null) {
                metadataMap.put(schema.getTopicName(), schema);
            }
        }
        return metadataMap;
    }
}