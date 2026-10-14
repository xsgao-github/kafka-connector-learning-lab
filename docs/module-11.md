# Module 12: Kafka Client SASL Authentication & Security

## Topic
Configuring SASL Authentication (PLAIN, SCRAM, and GSSAPI/Kerberos) and TLS Encryption for Java Kafka Connectors & Clients

## Key Kafka & Security Concepts

* **Security Protocols**: Configures the underlying connection transport using `security.protocol` (`SASL_PLAINTEXT` for unencrypted SASL authentication or `SASL_SSL` for encrypted transport).
* **JAAS Configuration**: Injects authentication credentials programmatically into Java clients using `sasl.jaas.config` via inline JAAS module declarations.
* **Authentication Mechanisms**:
  * **SASL/PLAIN**: Lightweight username/password authentication; must be paired with SSL/TLS (`SASL_SSL`) in production to protect credentials in transit.
  * **SASL/SCRAM**: Challenge-response mechanism (`SCRAM-SHA-256` or `SCRAM-SHA-512`) storing salted password hashes securely.
  * **SASL/GSSAPI**: Enterprise Kerberos authentication using ticket granting tickets (TGT) and keytab files.
* **TLS/SSL Encryption**: Secures data in transit using client-side TrustStores (`ssl.truststore.location`) and KeyStores (`ssl.keystore.location`).

## Implementation Details

### 1. Client Configuration Properties

Kafka client configuration relies on core keys from `CommonClientConfigs`, `SaslConfigs`, and `SslConfigs`:

```java
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.config.SaslConfigs;
import org.apache.kafka.config.SslConfigs;
import java.util.Properties;

Properties props = new Properties();
props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kafka.example.com:9093");

// Transport Protocol
props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
```

### 2. SASL/PLAIN Configuration (Username & Password)

```java
props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
props.put(SaslConfigs.SASL_JAAS_CONFIG,
    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
    "username=\"your-key\" " +
    "password=\"your-secret\";");
```

### 3. SASL/SCRAM-SHA-256 / SCRAM-SHA-512 Configuration

```java
props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
props.put(SaslConfigs.SASL_JAAS_CONFIG,
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username=\"app-user\" " +
    "password=\"app-password\";");
```

### 4. SASL/GSSAPI (Kerberos) Configuration

```java
props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
props.put(SaslConfigs.SASL_JAAS_CONFIG,
    "com.sun.security.auth.module.Krb5LoginModule required " +
    "useKeyTab=true " +
    "storeKey=true " +
    "keyTab=\"/etc/security/keytabs/kafka.keytab\" " +
    "principal=\"kafka-client@REALM.COM\";");
```

## Common Pitfalls & Solutions

* **Insecure PLAIN Credentials over Plaintext**: **Problem**: Using `SASL_PLAINTEXT` with `PLAIN` mechanism transmits credentials unencrypted across the network. **Solution**: Always enforce `SASL_SSL` when using `PLAIN` authentication.
* **SSL Certificate Verification Failures**: **Problem**: Untrusted self-signed broker certificates trigger `SSLHandshakeException`. **Solution**: Point `ssl.truststore.location` and `ssl.truststore.password` to the PEM or JKS file containing the broker's CA certificate.

## Verification

* **Verify Connection**: Run the app with port `9003`, e.g. `java -jar target/kafka-connector-learning-lab-1.0-SNAPSHOT.jar --metadata metadata.json --bootstrap localhost:9003 --module 11`, verify the client authenticates against Kafaka server `[Module 11] Subscribed to topic 'users_topic' on localhost:9003 over SASL_PLAINTEXT as user 'alice'`.
