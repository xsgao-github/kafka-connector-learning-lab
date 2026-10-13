# Kafka Connector Learning Lab

A hands-on project designed to explore the depths of Apache Kafka programming by building a production-grade JDBC-like connector. This repository serves as a living laboratory to evaluate Kafka features, version differences, and architectural patterns.

## 🎯 Project Goal
To transform a simple Kafka consumer into a high-performance, schema-aware data connector, implementing the same patterns used by industrial-strength tools like the Trino Kafka Connector.

- [Introduction Video](https://www.youtube.com/watch?v=jzd-LlsWw0s)

## 🚀 Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Java 17+](https://adoptium.net/)
- [Maven](https://maven.apache.org/download.cgi)
- [Python 3.x](https://www.python.org/downloads/)

### Setup & Run
1. **Initialize Kafka Containers**:
   ```bash
   docker compose up -d
   ```

2. **Create Topic and Seed Data**:
   ```bash
   python setup_env.py
   ```

3. **Build the Project**:
   ```bash
   mvn clean package -DskipTests
   ```

4. **Run the Connector**:
   Run the generate `.jar` with `--module <id>` to specify which module to use.
   ```bash
   java -jar target/kafka-connector-learning-lab-1.0-SNAPSHOT.jar --metadata metadata.json --bootstrap localhost:9092 --module 0
   ```
   
   **Arguments:**
   - `--metadata <path>`: Path to the JSON metadata file (Required)
   - `--bootstrap <servers>`: Kafka bootstrap servers (Required)
   - `--module <id>`: Module ID to execute (e.g., `0` for Baseline)

## 🗺️ Learning Roadmap
This project is organized as [a series of modules](https://notebook.google.com/notebook/7a96bf11-87c9-4f4c-9796-61f4b14c31d6/artifact/d060d43c-500e-4a26-b5ea-16606dda66d2?utm_source=nlm_web_share&utm_medium=google_oo&utm_campaign=art_share_1&utm_content=&utm_smc=nlm_web_share_google_oo_art_share_1_). Each module introduces a new Kafka concept, implements it in code, and documents the findings. There are no dependencies between modules, except for module 0, which sets up your environment. After module 0, you can jump to any module or topic as needed.

- [x] **Module 0**: Getting Started (Baseline) $\rightarrow$ [docs/module-0.md](docs/module-0.md)
- [x] **Module 1**: Offset Tracking $\rightarrow$ [docs/module-1.md](docs/module-1.md)
- [x] **Module 2**: Parallel Downloading $\rightarrow$ [docs/module-2.md](docs/module-2.md)
- [x] **Module 3**: Reliability & DLQs (Dead Letter Queue) $\rightarrow$ [docs/module-3.md](docs/module-3.md)
- [x] **Module 4**: Predicate Pushdown $\rightarrow$ [docs/module-4.md](docs/module-4.md)
- [x] **Module 5**: Exactly-Once (EOS) Delivery $\rightarrow$ [docs/module-5.md](docs/module-5.md)
- [x] **Module 6**: Schema Evolution & Registry Integration $\rightarrow$ [docs/module-6.md](docs/module-6.md)
- [x] **Module 7**: Observability (Consumer Lag & Metrics) $\rightarrow$ [docs/module-7.md](docs/module-7.md)
- [x] **Module 8**: Backpressure (pause() / resume()) $\rightarrow$ [docs/module-8.md](docs/module-8.md)
- [x] **Module 9**: Graceful Shutdown $\rightarrow$ [docs/module-9.md](docs/module-9.md)
- [x] **Module 10**: Consumer Pool $\rightarrow$ [docs/module-10.md](docs/module-10.md)
- [x] **Module 11**: SASL Authentication $\rightarrow$ [docs/module-11.md](docs/module-11.md)
- [x] **Module 12**: Integration Test $\rightarrow$ [docs/module-12.md](docs/module-12.md)

## 📚 Report and Resources
- [Tech Challenges](https://notebook.google.com/notebook/7a96bf11-87c9-4f4c-9796-61f4b14c31d6/artifact/5f150114-6c3f-4663-b92e-6f60e00f3667?utm_source=nlm_web_share&utm_medium=google_oo&utm_campaign=art_share_1&utm_content=&utm_smc=nlm_web_share_google_oo_art_share_1_)
- [Quiz](https://notebook.google.com/notebook/7a96bf11-87c9-4f4c-9796-61f4b14c31d6/artifact/b181a22b-052c-4213-8ec4-400cf4994f47?utm_source=nlm_web_share&utm_medium=google_oo&utm_campaign=art_share_1&utm_content=&utm_smc=nlm_web_share_google_oo_art_share_1_)
- [Flashcards](https://notebook.google.com/notebook/7a96bf11-87c9-4f4c-9796-61f4b14c31d6/artifact/50294ae1-a786-4d1f-bae9-5f6ff3dcd5c6?utm_source=nlm_web_share&utm_medium=google_oo&utm_campaign=art_share_1&utm_content=&utm_smc=nlm_web_share_google_oo_art_share_1_)
- [Mindmap](https://notebook.google.com/notebook/7a96bf11-87c9-4f4c-9796-61f4b14c31d6/artifact/facbb561-064e-47e3-9a42-bf26a1e62d8e?utm_source=nlm_web_share&utm_medium=google_oo&utm_campaign=art_share_1&utm_content=&utm_smc=nlm_web_share_google_oo_art_share_1_)
