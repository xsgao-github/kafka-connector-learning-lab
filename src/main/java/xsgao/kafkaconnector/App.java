package xsgao.kafkaconnector;

import xsgao.kafkaconnector.core.KafkaReader;
import xsgao.kafkaconnector.core.ReadingStrategy;
import xsgao.kafkaconnector.core.StrategyFactory;
import xsgao.kafkaconnector.metadata.MetadataLoader;
import xsgao.kafkaconnector.model.TableSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App {
    public static void main(String[] args) {
        String metadataPath = null;
        String bootstrapServers = null;
        int moduleId = 0;
        Long offset = null;

        // Simple Linux-style argument parser
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--metadata":
                    if (i + 1 < args.length) metadataPath = args[++i];
                    break;
                case "--bootstrap":
                    if (i + 1 < args.length) bootstrapServers = args[++i];
                    break;
                case "--module":
                    if (i + 1 < args.length) moduleId = Integer.parseInt(args[++i]);
                    break;
                case "--offset":
                    if (i + 1 < args.length) offset = Long.parseLong(args[++i]);
                    break;
                default:
                    System.out.println("Unknown argument: " + args[i]);
            }
        }

        if (metadataPath == null || bootstrapServers == null) {
            System.out.println("Usage: java -jar kafka-connector.jar --metadata <path> --bootstrap <servers> [--module <id>] [--offset <value>]");
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

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
