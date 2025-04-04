"""
Kafka consumer for processing transaction data.
"""
import json
import logging
import time
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.models.transaction import Transaction
from src.utils.config_loader import get_config, get_config_value


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionConsumer:
    """
    Kafka consumer for processing transaction data.
    """
    def __init__(
        self, 
        bootstrap_servers: Optional[str] = None, 
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        auto_offset_reset: str = 'earliest'
    ):
        """
        Initialize the TransactionConsumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (comma-separated)
            topic: Kafka topic to consume messages from
            group_id: Consumer group ID
            auto_offset_reset: Auto offset reset strategy ('earliest' or 'latest')
        """
        # Load configuration
        kafka_config = get_config("kafka_config")
        consumer_config = kafka_config.get("consumer", {})
        
        # Set bootstrap servers, topic, and group_id
        self.bootstrap_servers = bootstrap_servers or get_config_value(
            "kafka_config", "kafka.bootstrap_servers", "localhost:9092"
        )
        self.topic = topic or get_config_value(
            "kafka_config", "topics.input.transactions", "banking.transactions.raw"
        )
        self.group_id = group_id or consumer_config.get("group_id", "banking_etl_group")
        
        # Configure and create Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=consumer_config.get("enable_auto_commit", True),
            auto_commit_interval_ms=consumer_config.get("auto_commit_interval_ms", 5000),
            session_timeout_ms=consumer_config.get("session_timeout_ms", 30000),
            max_poll_records=consumer_config.get("max_poll_records", 500),
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
        logger.info(f"TransactionConsumer initialized with bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"Subscribed to topic: {self.topic}, group_id: {self.group_id}")
        
        # Processors for handling messages
        self.processors: List[Callable[[Transaction], None]] = []
    
    def add_processor(self, processor: Callable[[Transaction], None]) -> None:
        """
        Add a processor function to handle consumed transactions.
        
        Args:
            processor: Function that takes a Transaction object as input and processes it
        """
        self.processors.append(processor)
        logger.info(f"Added processor: {processor.__name__ if hasattr(processor, '__name__') else str(processor)}")
    
    def _process_message(self, message) -> None:
        """
        Process a Kafka message by converting it to a Transaction object and applying processors.
        
        Args:
            message: Kafka message
        """
        try:
            # Create Transaction object from message value
            transaction_data = message.value
            transaction = Transaction.from_dict(transaction_data)
            
            # Set processing timestamp
            transaction.processing_timestamp = datetime.now()
            
            # Apply all processors
            for processor in self.processors:
                processor(transaction)
                
            logger.debug(f"Processed transaction: {transaction.transaction_id}")
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
    
    def start_consuming(self, timeout_ms: int = None, max_records: int = None) -> None:
        """
        Start consuming messages from Kafka and process them.
        
        Args:
            timeout_ms: Maximum time to block waiting for messages (ms)
            max_records: Maximum number of records to consume (None for unlimited)
        """
        logger.info("Starting to consume messages...")
        
        try:
            record_count = 0
            start_time = time.time()
            
            # Consume messages
            for message in self.consumer:
                self._process_message(message)
                
                record_count += 1
                
                # Check if we've processed the maximum number of records
                if max_records is not None and record_count >= max_records:
                    logger.info(f"Reached maximum records ({max_records}). Stopping consumption.")
                    break
                    
                # Check if we've exceeded the timeout
                if timeout_ms is not None and (time.time() - start_time) * 1000 >= timeout_ms:
                    logger.info(f"Reached timeout ({timeout_ms}ms). Stopping consumption.")
                    break
                    
            logger.info(f"Consumed and processed {record_count} records")
            
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Stopping consumption.")
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}", exc_info=True)
            
    def close(self) -> None:
        """
        Close the Kafka consumer.
        """
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


def print_transaction(transaction: Transaction) -> None:
    """
    Example processor function that prints transaction details.
    """
    print(f"Transaction ID: {transaction.transaction_id}")
    print(f"Account ID: {transaction.account_id}")
    print(f"Type: {transaction.transaction_type}")
    print(f"Amount: {transaction.amount} {transaction.currency}")
    print(f"Status: {transaction.status}")
    print(f"Timestamp: {transaction.timestamp}")
    print("-" * 50)


if __name__ == "__main__":
    # Example usage
    consumer = TransactionConsumer()
    
    # Add sample processor
    consumer.add_processor(print_transaction)
    
    try:
        # Consume 20 messages or until timeout
        consumer.start_consuming(timeout_ms=30000, max_records=20)
    finally:
        consumer.close() 