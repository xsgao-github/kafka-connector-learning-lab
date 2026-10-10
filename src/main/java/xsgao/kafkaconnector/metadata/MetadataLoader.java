package xsgao.kafkaconnector.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import xsgao.kafkaconnector.model.ColumnType;
import xsgao.kafkaconnector.model.TableSchema;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads metadata from an external JSON file mapping Kafka topics to tables.
 */
public class MetadataLoader {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, TableSchema> loadMetadata(String filePath) throws IOException {
        File file = new File(filePath);
        List<TableSchema> schemas = objectMapper.readValue(file, new TypeReference<List<TableSchema>>() {});

        Map<String, TableSchema> topicToSchema = new HashMap<>();
        for (TableSchema schema : schemas) {
            // Ensure columns use the ColumnType enum
            for (TableSchema.ColumnDefinition col : schema.getColumns()) {
                // This handles the case where Jackson might have loaded the type as a string
                // but our model now expects a ColumnType.
                // Since we're using an enum, Jackson handles the conversion if the JSON string
                // matches the enum name. For safety, we can ensure it here.
            }
            topicToSchema.put(schema.getTopicName(), schema);
        }
        return topicToSchema;
    }
}
