package xsgao.kafkaconnector.model;

/**
 * Supported data types for the Kafka Connector.
 */
public enum ColumnType {
    STRING,
    INT,
    DOUBLE,
    BOOLEAN;

    public static ColumnType fromString(String type) {
        if (type == null) return STRING;
        try {
            return ColumnType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException e) {
            // Fallback for common aliases
            return switch (type.toUpperCase()) {
                case "INTEGER" -> INT;
                case "FLOAT" -> DOUBLE;
                default -> STRING;
            };
        }
    }
}
