package xsgao.kafkaconnector.core;

/**
 * Factory to instantiate the correct ReadingStrategy based on the module ID.
 */
public class StrategyFactory {
    public static ReadingStrategy getStrategy(int moduleId) {
        return switch (moduleId) {
            case 0 -> new DefaultReadingStrategy();
            default -> throw new IllegalArgumentException("Unsupported module ID: " + moduleId);
        };
    }
}
