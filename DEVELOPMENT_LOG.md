# Kafka Learning Roadmap

This repository is structured as a sequential learning path. Each module builds upon the previous one, adding a new layer of production-grade complexity to the Kafka Connector.

## 🗺️ Comprehensive Curriculum

| Module | Topic | Learning Objective | Status | Documentation |
| :--- | :--- | :--- | :--- | :--- |
| **0** | **Getting Started** | Baseline connectivity & basic CSV parsing. | ✅ | [docs/module-0.md](docs/module-0.md) |
| **1** | **Offset Tracking** | Persistent state & resuming from checkpoints. | ⏳ | TBD |
| **2** | **Parallelism** | Partitions, Consumer Groups, and multi-threading. | ⏳ | TBD |
| **3** | **Reliability & DLQs** | Handling malformed data via Dead Letter Queues. | ⏳ | TBD |
| **4** | **Predicate Pushdown** | Optimization: filtering data at the source. | ⏳ | TBD |
| **5** | **Exactly-Once (EOS)** | Ensuring data integrity and no duplicates. | ⏳ | TBD |
| **6** | **Schema Evolution** | Confluent Schema Registry & Avro/Protobuf. | ⏳ | TBD |
| **7** | **Observability** | Tracking Consumer Lag and throughput metrics. | ⏳ | TBD |

## 🛠️ Learning Process
For each module, we follow this cycle:
1. **Theory**: Understand the Kafka concept (e.g., Consumer Groups).
2. **Implementation**: Modify the connector to support the feature.
3. **Experimentation**: Produce "edge case" data to see where the implementation breaks.
4. **Documentation**: Record the "Hiccups & Tricks" in the module's dedicated documentation file.
