# Module 12: Integration Testing with Testcontainers & GitHub Actions

## Topic
Containerized Integration Testing for Apache Kafka Pipelines using Testcontainers and GitHub Actions CI/CD

## Key Kafka & Testing Concepts

* **Ephemeral Containerized Clusters**: Programmatically spins up isolated, disposable Kafka brokers using `org.testcontainers.containers.KafkaContainer` within JUnit 5 test suites—eliminating reliance on pre-existing local Docker Compose setups.
* **JUnit 5 Extension Integration**: Uses `@Testcontainers` and `@Container` annotations to declaratively manage container startup, health checks, and automatic teardown across the test lifecycle.
* **Decoupled Verification Pattern**: Tests raw Kafka cluster mechanics (topic provisioning via `AdminClient`, record ingestion via `KafkaProducer`, and poll loop via `KafkaConsumer`) independently of custom application strategies or domain logic.
* **Dynamic Host Port Mapping**: Automatically maps container internal ports (e.g., `9092`) to available random ephemeral host ports (`kafka.getBootstrapServers()`), preventing port collisions during parallel builds.
* **Cross-Platform Continuous Integration**: Executes hermetic integration tests on GitHub-hosted Linux runners (`ubuntu-latest`) using the native Docker socket (`/var/run/docker.sock`).

## Common Pitfalls & Solutions

* **Docker Daemon Connection Failures on Local Machines**: On Linux CI, Docker works out of the box via `/var/run/docker.sock`. On local Windows machines, configure TCP daemon access or environment settings (WIP).
* **Docker Engine API Version Mismatch (`client version 1.32 is too old`)**: Legacy fallback API versions rejected by modern Docker Engine 25+. **Fix**: Align Testcontainers dependencies to version `1.20.4+` or set `DOCKER_API_VERSION=1.40`.
* **Hardcoding Host Specific Ports**: Hardcoding port `9092` causes conflicts with active local Kafka instances. **Fix**: Always fetch dynamically assigned bootstrap ports via `KAFKA.getBootstrapServers()`.

## Verification

* **Execute Local Test**: Run `mvn test -Dtest=IntegrationTest` on local.
* **Execute CI Pipeline**:
  Push changes to GitHub and monitor execution under the **Actions** tab of your repository.
* **Expected Output**: Ephemeral Kafka Testcontainer launches, creates `demo-topic`, seeds 3 records, polls and displays records, and terminates automatically.
