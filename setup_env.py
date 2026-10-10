import subprocess
import time
import sys

# Configuration
# Use the internal Kafka listener for docker exec commands
INTERNAL_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_NAME = "users_topic"
PARTITIONS = 3
REPLICATION_FACTOR = 1
KAFKA_CONTAINER = "kafka-connector-learning-lab-kafka-1"

def run_command(cmd, input_data=None):
    """Helper to run shell commands via docker exec."""
    try:
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True
        )
        stdout, stderr = process.communicate(input=input_data)
        return stdout, stderr, process.returncode
    except Exception as e:
        print(f"Command failed: {e}")
        return None, str(e), 1

def main():
    print("--- Starting Kafka Environment (Python Edition) ---")

    # 1. Start Docker containers
    print("Starting Docker containers...")
    subprocess.run(["docker", "compose", "up", "-d"])

    print("Waiting for Kafka to be ready...", end="")
    while True:
        # Check if Kafka is responsive using internal bootstrap server
        cmd = f"docker exec {KAFKA_CONTAINER} kafka-topics --bootstrap-server {INTERNAL_BOOTSTRAP_SERVER} --list"
        stdout, stderr, code = run_command(cmd)
        if code == 0:
            break
        print(".", end="", flush=True)
        time.sleep(2)
    print("\nOK: Kafka is up and running!")

    # 2. Handle Topic Creation
    print(f"Checking for topic {TOPIC_NAME}...")
    cmd_list = f"docker exec {KAFKA_CONTAINER} kafka-topics --bootstrap-server {INTERNAL_BOOTSTRAP_SERVER} --list"
    stdout, _, _ = run_command(cmd_list)

    if TOPIC_NAME not in stdout:
        print(f"Creating topic {TOPIC_NAME} with {PARTITIONS} partitions...")
        cmd_create = (f"docker exec {KAFKA_CONTAINER} kafka-topics --bootstrap-server {INTERNAL_BOOTSTRAP_SERVER} "
                      f"--create --topic {TOPIC_NAME} --partitions {PARTITIONS} "
                      f"--replication-factor {REPLICATION_FACTOR}")
        run_command(cmd_create)
        print("OK: Topic created.")
    else:
        print(f"Info: Topic {TOPIC_NAME} already exists. Skipping creation.")

    # 3. Produce Golden Test Data
    # We include some "poison pills" (malformed data) here to prepare for Module 3
    print("Producing test records (including malformed data)...")
    test_data = (
        "1, Alice, 100.50\n"
        "2, Bob, 250.75\n"
        "3, Charlie, 10.00\n"
        "Diana, 500.00\n"   # Malformed: Missing ID (Module 3 test case)
        "5, Edward, 12.34\n"
        "6, Fiona, 789.0\n"
        "7, George, invalid\n" # Malformed: Type mismatch (Module 3 test case)
    )
    cmd_prod = f"docker exec -i {KAFKA_CONTAINER} kafka-console-producer --bootstrap-server {INTERNAL_BOOTSTRAP_SERVER} --topic {TOPIC_NAME}"
    run_command(cmd_prod, input_data=test_data)
    print("OK: Test data produced.")

    print("\n" + "-"*50)
    print("Environment ready!")
    print("You can now run the app:")
    print("java -jar target/kafka-connector-learning-lab-1.0-SNAPSHOT.jar --metadata metadata.json --bootstrap localhost:9092 --module 0")
    print("-"*50)

if __name__ == "__main__":
    main()

