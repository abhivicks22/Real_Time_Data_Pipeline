#!/bin/bash
# Script to start Kafka and create required topics

# Exit on any error
set -e

# Load configuration
KAFKA_HOME=${KAFKA_HOME:-~/kafka}
BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-localhost:9092}

# Define topics
INPUT_TOPICS=("banking.transactions.raw" "banking.accounts.raw" "banking.customers.raw")
OUTPUT_TOPICS=("banking.transactions.validated" "banking.transactions.enriched" "banking.alerts")
INTERNAL_TOPICS=("banking.deadletter" "banking.retry")

# Check if Kafka is already running
check_kafka() {
    echo "Checking if Kafka is running..."
    nc -z localhost 9092 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "Kafka is already running on port 9092"
        return 0
    else
        echo "Kafka is not running"
        return 1
    fi
}

# Start ZooKeeper
start_zookeeper() {
    echo "Starting ZooKeeper..."
    cd $KAFKA_HOME
    bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
    sleep 5
    echo "ZooKeeper started"
}

# Start Kafka
start_kafka() {
    echo "Starting Kafka broker..."
    cd $KAFKA_HOME
    bin/kafka-server-start.sh -daemon config/server.properties
    sleep 10
    echo "Kafka broker started"
}

# Create topics
create_topics() {
    echo "Creating Kafka topics..."
    cd $KAFKA_HOME
    
    # Create input topics
    for topic in "${INPUT_TOPICS[@]}"; do
        echo "Creating topic: $topic"
        bin/kafka-topics.sh --create --if-not-exists \
            --bootstrap-server $BOOTSTRAP_SERVERS \
            --replication-factor 1 \
            --partitions 3 \
            --topic $topic \
            --config retention.ms=86400000
    done
    
    # Create output topics
    for topic in "${OUTPUT_TOPICS[@]}"; do
        echo "Creating topic: $topic"
        bin/kafka-topics.sh --create --if-not-exists \
            --bootstrap-server $BOOTSTRAP_SERVERS \
            --replication-factor 1 \
            --partitions 3 \
            --topic $topic \
            --config retention.ms=86400000
    done
    
    # Create internal topics
    for topic in "${INTERNAL_TOPICS[@]}"; do
        echo "Creating topic: $topic"
        bin/kafka-topics.sh --create --if-not-exists \
            --bootstrap-server $BOOTSTRAP_SERVERS \
            --replication-factor 1 \
            --partitions 1 \
            --topic $topic \
            --config retention.ms=86400000
    done
    
    echo "All topics created successfully"
}

# List topics
list_topics() {
    echo "Listing all Kafka topics:"
    cd $KAFKA_HOME
    bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVERS
}

# Main logic
main() {
    if ! check_kafka; then
        echo "Starting Kafka environment..."
        start_zookeeper
        start_kafka
    fi
    
    create_topics
    list_topics
    
    echo "Kafka setup completed successfully"
}

# Run the main function
main 