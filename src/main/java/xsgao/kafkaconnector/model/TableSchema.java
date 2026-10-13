package xsgao.kafkaconnector.model;

import java.util.List;
import java.util.Map;

/**
 * Represents the schema of a Kafka topic mapped to a table.
 */
public class TableSchema {
    private String tableName;
    private String topicName;
    private List<ColumnDefinition> columns;
    private Map<String, List<ColumnDefinition>> versions;

    public TableSchema() {}

    public TableSchema(String tableName, String topicName, List<ColumnDefinition> columns) {
        this.tableName = tableName;
        this.topicName = topicName;
        this.columns = columns;
    }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getTopicName() { return topicName; }
    public void setTopicName(String topicName) { this.topicName = topicName; }

    public List<ColumnDefinition> getColumns() { return columns; }
    public void setColumns(List<ColumnDefinition> columns) { this.columns = columns; }

    public Map<String, List<ColumnDefinition>> getVersions() { return versions; }
    public void setVersions(Map<String, List<ColumnDefinition>> versions) { this.versions = versions; }

    public static class ColumnDefinition {
        private String name;
        private ColumnType type; // e.g., STRING, INT, DOUBLE

        public ColumnDefinition() {}

        public ColumnDefinition(String name, ColumnType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public ColumnType getType() { return type; }
        public void setType(ColumnType type) { this.type = type; }
    }
}
