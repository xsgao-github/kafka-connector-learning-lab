package xsgao.kafkaconnector.core;

import java.util.*;

/**
 * Strategy interface for reading data from Kafka.
 * Different implementations allow for different consumption behaviors.
 */
public interface ReadingStrategy {
    /**
     * Reads records from Kafka and converts them into a result set.
     *
     * @param tableName   The logical name of the table to read from.
     * @param limit       Maximum number of records to retrieve.
     * @param consumerProps The Kafka consumer configuration properties.
     * @param tableToSchema Mapping of table names to their respective schemas.
     * @param params      Additional parameters (e.g., 'offset', 'readMode').
     * @return A list of rows, where each row is a list of casted objects.
     */
    List<List<Object>> read(
        String tableName,
        int limit,
        Properties consumerProps,
        Map<String, xsgao.kafkaconnector.model.TableSchema> tableToSchema,
        Map<String, Object> params
    );
}
