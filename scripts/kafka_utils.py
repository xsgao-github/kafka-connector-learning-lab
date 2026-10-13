import subprocess
import time
import logging

# Centralized Constants
KAFKA_CONTAINER = "kafka-connector-learning-lab-kafka-1"
INTERNAL_BOOTSTRAP_SERVER = "kafka:29092"
USERS_TOPIC = "users_topic"
USERS_DLQ_TOPIC = "users_topic_dlq"
ORDERS_TOPIC = "orders_topic"
ORDERS_DLQ_TOPIC = "orders_topic_dlq"
DEFAULT_PARTITIONS = 3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaUtils")

def get_existing_topics():
    """Lists currently existing Kafka topics inside the container."""
    cmd = (
        f"docker exec {KAFKA_CONTAINER} kafka-topics "
        f"--bootstrap-server {INTERNAL_BOOTSTRAP_SERVER} --list"
    )
    try:
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if res.returncode == 0:
            return [t.strip() for t in res.stdout.splitlines() if t.strip()]
    except Exception as e:
        logger.warning(f"Failed to list topics: {e}")
    return []

def delete_topics_if_exist(topic_names):
    """Deletes Kafka topics only if they currently exist to prevent Java stacktrace errors."""
    existing_topics = get_existing_topics()
    for topic in topic_names:
        if topic in existing_topics:
            cmd = (
                f"docker exec {KAFKA_CONTAINER} kafka-topics "
                f"--bootstrap-server {INTERNAL_BOOTSTRAP_SERVER} "
                f"--delete --topic {topic}"
            )
            try:
                res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if res.returncode == 0:
                    logger.info(f"Deleted topic: '{topic}'")
                else:
                    logger.warning(f"Notice deleting topic '{topic}': {res.stderr.strip()}")
            except Exception as e:
                logger.warning(f"Failed to execute delete for topic '{topic}': {e}")
    time.sleep(1)

def ensure_topic_exists(topic_name, num_partitions=DEFAULT_PARTITIONS):
    """Creates a topic if it does not exist via docker exec kafka-topics."""
    cmd = (
        f"docker exec {KAFKA_CONTAINER} kafka-topics "
        f"--bootstrap-server {INTERNAL_BOOTSTRAP_SERVER} "
        f"--create --if-not-exists "
        f"--topic {topic_name} "
        f"--partitions {num_partitions} "
        f"--replication-factor 1"
    )
    try:
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if res.returncode == 0:
            logger.info(f"Ensured topic exists: '{topic_name}' ({num_partitions} partitions)")
        else:
            logger.error(f"Error creating topic '{topic_name}': {res.stderr.strip()}")
    except Exception as e:
        logger.error(f"Failed to ensure topic '{topic_name}': {e}")

def send_string_records(topic, records, with_header=False):
    """Publishes string records to a topic via docker exec kafka-console-producer."""
    # Simply setting parse.headers=true is enough (defaults to \t for headers delimiter)
    header_flags = " --property parse.headers=true" if with_header else ""
    
    cmd = (
        f"docker exec -i {KAFKA_CONTAINER} kafka-console-producer "
        f"--bootstrap-server {INTERNAL_BOOTSTRAP_SERVER} "
        f"--topic {topic}"
        f"{header_flags} "
        f"--property parse.key=true "
        f"--property key.separator=:"
    )
    data = "\n".join(records) + "\n"
    try:
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True
        )
        stdout, stderr = process.communicate(input=data)

        if process.returncode == 0:
            logger.info(f"Successfully sent {len(records)} records to topic '{topic}'.")
            for r in records:
                logger.info(f"Sent: {r}")
            return len(records)
        else:
            logger.error(f"Error producing messages to '{topic}': {stderr.strip()}")
            return 0
    except Exception as e:
        logger.error(f"Exception while producing to '{topic}': {e}")
        return 0