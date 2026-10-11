# Module 2: Parallel Downloading

## Topic
Parallel Downloading

## Key Kafka Concepts
- **Consumer Is Not Thread-safe**: Don't access one consumer from multiple threads.
- **One or Multiple Parition Per Consumer**: One consumer can be assigned with one or more partitions, not not zero. For best performance, assign only one partition per consumer.
- **Resource Management**: Ensuring proper shutdown of consumers and executors to prevent resource leaks.

## Implementation Details
The `ParallelReadingStrategy` employs a parallel architecture to read data from a Kafka topic faster than a single consumer could.

### Architecture Overview
The strategy works as follows:
1. **Discovery**: A temporary Kafka consumer is used to discover all partitions belonging to the target topic.
2. **Distribution**: These partitions are divided into groups, where each group is assigned to a worker thread.
3. **Parallel Polling**: Each worker thread initializes its own `KafkaConsumer` instance and uses `consumer.assign(partitions)` to take direct control of its assigned subset of partitions.
4. **Collection**: Each thread reads records independently, and the results are collected into a synchronized list.
5. **Termination**: The process uses an `ExecutorService` and conditional waiting (`Thread.sleep` and `awaitTermination`) to gracefully shut down and wait until the required record limit is met or a timeout occurs.

### Code Snippet Focus: Partition Assignment
The core logic involves determining the number of partitions and distributing them:
```java
// ... inside ParallelReadingStrategy.read()
// Obtain metadata to distribute partitions across threads
List<TopicPartition> allPartitions = getAllPartitions(props, topic);
int totalPartitions = allPartitions.size();

// Best Practice: Cap active threads to total partitions.
// WHY: Having more consumer threads than partitions leads to idle worker threads.
int activeThreads = Math.min(requestedThreads, totalPartitions);

// Partition distribution map: workerIndex -> list of assigned TopicPartitions
Map<Integer, List<TopicPartition>> threadPartitionMap = new HashMap<>();
for (int i = 0; i < activeThreads; i++) {
    threadPartitionMap.put(i, new ArrayList<>());
}
for (int i = 0; i < totalPartitions; i++) {
    threadPartitionMap.get(i % activeThreads).add(allPartitions.get(i));
}

// Shared thread-safe structures across workers to handle partition skew cleanly
AtomicInteger globalRecordsFetched = new AtomicInteger(0);
List<List<Object>> validRecords = Collections.synchronizedList(new ArrayList<>());

ExecutorService executor = Executors.newFixedThreadPool(activeThreads);
List<Future<?>> futures = new ArrayList<>();

try {
    // Spawn dedicated consumer worker per thread
    for (int i = 0; i < activeThreads; i++) {
        List<TopicPartition> assignedPartitions = threadPartitionMap.get(i);
        
        Runnable worker = () -> pollPartitionSubset(
                topic, 
                assignedPartitions, 
                props, 
                targetSchema, 
                limit, 
                globalRecordsFetched, 
                validRecords, 
                params
        );
        futures.add(executor.submit(worker));
    }

    // Await completion of all worker threads
    for (Future<?> future : futures) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Parallel reading task interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error executing parallel consumer worker", e.getCause());
        }
    }
} finally {
    // Best Practice: Always shutdown ExecutorService in a finally block.
    // WHY: Prevents thread pool leak and releases worker resources cleanly upon exit or exception.
    executor.shutdown();
}
```

## Common Pitfalls & Solutions
- **Consumer Independence**: Each worker must maintain its own `KafkaConsumer` instance. Sharing a single consumer across threads for polling/assignment is not thread-safe and invalidates the partitioning scheme.
- **Synchronization**: Since multiple threads write to a shared results list, `Collections.synchronizedList()` must be used to prevent data corruption.
- **Graceful Shutdown**: Proper use of `executor.shutdown()` followed by `executor.awaitTermination()` is crucial for ensuring all processing threads complete their tasks before the application exits.

## Verification
- **Execution**: Run the application specifying Module 2.
- **Expected Result**: Data is read efficiently, significantly faster than single-threaded consumption, reflecting high throughput across multiple partitions.

