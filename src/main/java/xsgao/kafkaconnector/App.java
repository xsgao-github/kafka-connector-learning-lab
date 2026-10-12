package xsgao.kafkaconnector;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import xsgao.kafkaconnector.core.KafkaReader;
import xsgao.kafkaconnector.core.ReadingStrategy;
import xsgao.kafkaconnector.core.StrategyFactory;
import xsgao.kafkaconnector.metadata.MetadataLoader;
import xsgao.kafkaconnector.model.TableSchema;

public class App {
    public static void main(String[] args) {
        String metadataPath = null;
        String bootstrapServers = null;
        int moduleId = 0;
        Long offset = null;
        Long timestamp = null;
        boolean useRebalance = false;
        boolean readDlq = false;
        String filter = null;

        // Simple Linux-style argument parser
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--metadata" -> {
                    if (i + 1 < args.length) metadataPath = args[++i];
                }
                case "--bootstrap" -> {
                    if (i + 1 < args.length) bootstrapServers = args[++i];
                }
                case "--module" -> {
                    if (i + 1 < args.length) moduleId = Integer.parseInt(args[++i]);
                }
                case "--offset" -> {
                    if (i + 1 < args.length) offset = Long.valueOf(args[++i]);
                }
                case "--timestamp" -> {
                    if (i + 1 < args.length) {
                        String tsValue = args[++i];
                        try {
                            // Try to parse as Epoch Millis first
                            timestamp = Long.valueOf(tsValue);
                        } catch (NumberFormatException e) {
                            // Fallback to YYYY-MM-dd HH:mm:ss
                            try {
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                LocalDateTime ldt = LocalDateTime.parse(tsValue, formatter);
                                timestamp = ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            } catch (DateTimeParseException ex) {
                                System.err.println("Invalid timestamp format. Use epoch millis or 'yyyy-MM-dd HH:mm:ss'");
                                return;
                            }
                        }
                    }
                }
                case "--rebalance" -> {
                    useRebalance = true;
                }
                case "--read_dlq" -> {
                    readDlq = true;
                }
                case "--filter" -> {
                    if (i + 1 < args.length) {
                        filter = args[++i];
                    }
                }
                default -> System.out.println("Unknown argument: " + args[i]);
            }
        }

        if (metadataPath == null || bootstrapServers == null) {
            System.out.println("Usage: java -jar kafka-connector.jar --metadata <path> --bootstrap <servers> [--module <id>]"
                    + " [--offset <value>] [--timestamp \"yyyy-MM-dd HH:mm:ss\"] [--rebalance] [--read_dlq] [--filter <expression>]");
            return;
        }

        try {
            // 1. Load Metadata
            MetadataLoader loader = new MetadataLoader();
            Map<String, TableSchema> metadata = loader.loadMetadata(metadataPath);
            System.out.println("Metadata loaded successfully. Mappings found: " + metadata.size());

            // 2. Prepare Parameters
            Map<String, Object> params = new HashMap<>();
            if (offset != null) {
                params.put("offset", offset);
            }
            if (timestamp != null) {
                params.put("timestamp", timestamp);
            }
            if (useRebalance) {
                params.put("useRebalanceListener", true);
            }
            if (readDlq) {
                params.put("read_dlq", true);
            }
            if (filter != null) {
                params.put("filter", filter);
            }

            // 3. Initialize Kafka Reader with Strategy
            ReadingStrategy strategy = StrategyFactory.getStrategy(moduleId);
            KafkaReader reader = new KafkaReader(bootstrapServers, metadata, strategy);

            String firstTable = metadata.values().iterator().next().getTableName();
            System.out.println("Running Module " + moduleId + " reading from table: " + firstTable);

            List<List<Object>> data = reader.read(firstTable, 10, params);

            System.out.println("\n--- Results ---");
            for (List<Object> row : data) {
                System.out.println(row);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
