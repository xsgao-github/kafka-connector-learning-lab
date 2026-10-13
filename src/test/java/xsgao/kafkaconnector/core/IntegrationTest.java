package xsgao.kafkaconnector.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Standalone Integration Test using Testcontainers & JUnit 5.
 * Key Features:
 * <ul>
 *   <li><b>Decoupled Architecture:</b> Uses pure Apache Kafka client APIs (AdminClient, KafkaProducer, KafkaConsumer) without depending on any custom strategy classes.</li>
 *   <li><b>Container Lifecycle:</b> Starts an ephemeral Kafka broker container automatically via Testcontainers.</li>
 *   <li><b>Topic & Data Provisioning:</b> Programmatically creates a topic and produces 3 records.</li>
 *   <li><b>Consumption & Display:</b> Polls messages with a standard Kafka Consumer and prints fetched key-value records.</li>
 * </ul>
 */
@Testcontainers
public class IntegrationTest {

    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.5.0";
    private static final String TOPIC_NAME = "demo-topic";
    private static final int EXPECTED_RECORD_COUNT = 3;

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE));

    @BeforeAll
    static void setUpClusterAndSeedData() throws Exception {
        // Ensure container is started and ready
        if (!KAFKA.isRunning()) {
            System.out.println("[Testcontainers] Explicitly starting Kafka container...");
            KAFKA.start();
        }

        String bootstrapServers = KAFKA.getBootstrapServers();
        System.out.printf("[Testcontainers] Kafka container active at: %s%n", bootstrapServers);

        // 1. Synchronous Topic Creation (15s timeout)
        System.out.println("[Step 1] Creating Kafka topic: " + TOPIC_NAME);
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        try (AdminClient admin = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
            admin.createTopics(Collections.singletonList(newTopic)).all().get(15, TimeUnit.SECONDS);
            System.out.printf("[AdminClient] Successfully created topic '%s'%n", TOPIC_NAME);
        }

        // 2. Synchronous Producer Publishing
        System.out.println("[Step 2] Publishing " + EXPECTED_RECORD_COUNT + " test records to Kafka...");
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 1; i <= EXPECTED_RECORD_COUNT; i++) {
                String key = "key-" + i;
                String value = "hello-kafka-record-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);

                // Wait for broker ack
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);

                System.out.printf("[KafkaProducer] Confirmed Record %d -> Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                        i, metadata.partition(), metadata.offset(), key, value);
            }
            producer.flush();
        }
    }

    @Test
    @DisplayName("Verify Kafka startup, topic creation, record production, and consumer polling")
    void testKafkaBasicConsumer() {
        String bootstrapServers = KAFKA.getBootstrapServers();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "integration-test-group-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> receivedRecords = new ArrayList<>();

        // 3. Poll and display the records
        System.out.println("[Step 3] Subscribing consumer and polling records...");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            long timeoutDeadline = System.currentTimeMillis() + 15000;
            while (receivedRecords.size() < EXPECTED_RECORD_COUNT && System.currentTimeMillis() < timeoutDeadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[KafkaConsumer] Fetched -> Key: %s | Value: %s | Partition: %d | Offset: %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                    receivedRecords.add(record);
                }
            }
        }

        assertEquals(EXPECTED_RECORD_COUNT, receivedRecords.size(),
                "Expected to consume exactly 3 records from Kafka");
    }
}