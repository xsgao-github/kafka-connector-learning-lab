# Kafka Connector Learning Lab

A hands-on project designed to explore the depths of Apache Kafka programming by building a production-grade JDBC-like connector. This repository serves as a living laboratory to evaluate Kafka features, version differences, and architectural patterns.

## 🎯 Project Goal
To transform a simple Kafka consumer into a high-performance, schema-aware data connector, implementing the same patterns used by industrial-strength tools like the Trino Kafka Connector.

## 🚀 Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Java 17+](https://adoptium.net/)
- [Maven](https://maven.apache.org/download.cgi)
- [Python 3.x](https://www.python.org/downloads/)

### Setup & Run
1. **Initialize Environment**:
   Run the automation script to start Kafka, create topics, and seed test data.
   ```bash
   python setup_env.py
   ```

2. **Build the Project**:
   ```bash
   mvn clean package
   ```

3. **Run the Connector**:
   The connector now supports named arguments for better control over the reading module and parameters.
   ```bash
   java -jar target/kafka-connector-learning-lab-1.0-SNAPSHOT.jar --metadata metadata.json --bootstrap localhost:9092 --module 0
   ```
   
   **Arguments:**
   - `--metadata <path>`: Path to the JSON metadata file (Required)
   - `--bootstrap <servers>`: Kafka bootstrap servers (Required)
   - `--module <id>`: Module ID to execute (e.g., `0` for Baseline)

## 📚 Learning Roadmap
This project is organized as a series of modules. Each module introduces a new Kafka concept, implements it in the code, and documents the findings.

- [x] **Module 0**: Getting Started (Baseline) $\rightarrow$ [docs/module-0.md](docs/module-0.md)
- [x] **Module 1**: Offset Tracking $\rightarrow$ [docs/module-1.md](docs/module-1.md)
- [x] **Module 2**: Parallel Downloading $\rightarrow$ [docs/module-2.md](docs/module-2.md)
- [x] **Module 3**: Reliability & DLQs (Dead Letter Queue) $\rightarrow$ [docs/module-3.md](docs/module-3.md)
- [x] **Module 4**: Predicate Pushdown $\rightarrow$ [docs/module-4.md](docs/module-4.md)
- [ ] **Module 5**: Exactly-Once (EOS) Delivery
- [ ] **Module 6**: Schema Evolution & Registry Integration
- [ ] **Module 7**: Observability (Consumer Lag & Metrics)
