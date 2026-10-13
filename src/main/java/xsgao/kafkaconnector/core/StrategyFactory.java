package xsgao.kafkaconnector.core;

/**
 * Factory to instantiate the correct ReadingStrategy based on the module ID.
 */
public class StrategyFactory {
    public static ReadingStrategy getStrategy(int moduleId) {
        return switch (moduleId) {
            case 0 -> new DefaultReadingStrategy();
            case 1 -> new OffsetReadingStrategy();
            case 2 -> new ParallelReadingStrategy();
            case 3 -> new ReliableReadingStrategy();
            case 4 -> new PredicateReadingStrategy();
            case 5 -> new ExactlyOnceReadingStrategy();
            default -> throw new IllegalArgumentException("Unsupported module ID: " + moduleId);
        };
    }
}
