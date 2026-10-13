# Module 6: Schema Discovery & Evolution

## Topic
Header-Based Schema Evolution & Dynamic Backward Compatibility

## Key Kafka Concepts
* **Header-Based Metadata**: Transporting payload version identifiers (`schema_version=v2`, `schema_version=v3`) inside Kafka record headers without modifying the raw message payload body.
* **Invariant Legacy Fallback (`No Header == V1`)**: Enforcing the rule that any record lacking a `schema_version` header is strictly parsed using the legacy 3-column `v1` schema contract (`id`, `name`, `balance`).
* **Versioned Schema Lookup**: Dynamically mapping incoming record version headers (`v1`, `v2`, `v3`) to versioned column metadata in `metadata.json`.
* **Widest Target Projection & Null Padding**: Dynamically resolving destination schemas (`resolveTargetColumns`) to the widest defined version (e.g., 5 columns in `v3`) and projecting `v1` and `v2` records into it by padding missing fields with `null`.
* **Additive Backward Compatibility**: Enabling consumers to process mixed-version streams (`v1`, `v2`, `v3`) simultaneously without breaking downstream database writer contracts.

## Comparison: Standard Wire Format vs. Custom Kafka Headers
| Aspect | Standard Method: Confluent Wire Format | Module 6 Approach: Kafka Record Headers |
| :--- | :--- | :--- |
| **Payload Structure** | Prepends a **5-byte header directly inside the payload body** [1]:<br>• **Byte 0 (Magic Byte):** Always `0`<br>• **Bytes 1–4 (Schema ID):** 4-byte integer ID | Uses native **Kafka Record Headers** (`schema_version=v2`) completely outside the payload. |
| **Schema Management** | Uses a central **Schema Registry** HTTP service (e.g., Confluent / AWS Glue) [1]. | Uses local configuration files (`metadata.json`). |
| **Schema Lookup** | Consumer extracts the 4-byte Schema ID from the payload, fetches the schema from the registry (or local cache), and deserializes [1, 2]. | Consumer extracts `schema_version` from record headers and matches `versions` in `metadata.json`. |
| **Serialization** | Binary formats (**Avro**, **Protobuf**, **JSON Schema**) [1]. | Text formats (**CSV**, **JSON**, plain text). |
| **Infrastructure** | Requires maintaining a separate Schema Registry cluster [1]. | **Zero extra infrastructure** (runs natively on standard Kafka brokers). |

## Implementation Details

### 1. `SchemaEvolutionStrategy.java`
Inspects Kafka record headers for `schema_version`, matches versioned column definitions, and normalizes records to the target table schema:

```java
private List<Object> processRecord(ConsumerRecord<String, String> record, TableSchema tableSchema) {
    if (record.value() == null) return null;

    // 1. Extract schema_version header (no header strictly defaults to "v1")
    String schemaVersion = extractSchemaVersion(record);

    // 2. Resolve input column mapping for this payload version
    List<TableSchema.ColumnDefinition> versionColumns = (tableSchema.getVersions() != null)
            ? tableSchema.getVersions().getOrDefault(schemaVersion, tableSchema.getColumns())
            : tableSchema.getColumns();

    // 3. Resolve destination schema (widest layout across all versions)
    List<TableSchema.ColumnDefinition> targetColumns = resolveTargetColumns(tableSchema);

    // 4. Parse CSV fields and project into target schema, padding missing fields with null
    String[] fields = record.value().split(",");
    List<Object> normalizedRow = new ArrayList<>();

    for (int i = 0; i < targetColumns.size(); i++) {
        if (i < fields.length && i < versionColumns.size()) {
            normalizedRow.add(parseFieldValue(fields[i].trim(), targetColumns.get(i)));
        } else {
            normalizedRow.add(null); // Pad missing fields (email, phone) for older versions
        }
    }
    return normalizedRow;
}

private List<TableSchema.ColumnDefinition> resolveTargetColumns(TableSchema tableSchema) {
    List<TableSchema.ColumnDefinition> target = tableSchema.getColumns();
    if (tableSchema.getVersions() != null) {
        for (List<TableSchema.ColumnDefinition> cols : tableSchema.getVersions().values()) {
            if (target == null || cols.size() > target.size()) {
                target = cols;
            }
        }
    }
    return target;
}
```

### 2. `metadata.json` (Nested Version Mapping)
```json
[
  {
    "tableName": "users",
    "topicName": "users_topic",
    "columns": [
      { "name": "id", "type": "INT" },
      { "name": "name", "type": "STRING" },
      { "name": "balance", "type": "DOUBLE" },
      { "name": "email", "type": "STRING" },
      { "name": "phone", "type": "STRING" }
    ],
    "versions": {
      "v1": [
        { "name": "id", "type": "INT" },
        { "name": "name", "type": "STRING" },
        { "name": "balance", "type": "DOUBLE" }
      ],
      "v2": [
        { "name": "id", "type": "INT" },
        { "name": "name", "type": "STRING" },
        { "name": "balance", "type": "DOUBLE" },
        { "name": "email", "type": "STRING" }
      ],
      "v3": [
        { "name": "id", "type": "INT" },
        { "name": "name", "type": "STRING" },
        { "name": "balance", "type": "DOUBLE" },
        { "name": "email", "type": "STRING" },
        { "name": "phone", "type": "STRING" }
      ]
    }
  }
]
```

## Common Pitfalls & Solutions
* For records without version tag, they are of v1, all later versions are required to have version tags.
* Maintained top-level `columns` to match the latest table layout, and used `versions[v].columns` in `SchemaEvolutionStrategy` to parse the exact incoming payload size, dynamically padding missing target columns with `null` for old version records.

## Verification
* **Seed V2 and V3 Data**: `python .\scripts\seed_module6.py`
* **Old Consumer Test (`--module 0`)**: Temporarily remove the last two columns from `columns` array in `metadatajson`, run app with `--module 0`. You should see `WARNING: Malformed record detected. Expected 3 columns, found 4. Record: [13,Mark,130.0,mark@example.com]` in the console and the final rows contain only 3 columns.
* **Evolved Consumer Test (`--module 6`)**: Restore the two columns in `metadata.json`, run app with `--module 6`. There will be no `WARNING` in the console, and all rwos contain 6 columns. For v1 and v2 records, `email` and `phone` columns are padded with `null`.
