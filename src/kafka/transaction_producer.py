"""
Kafka producer for generating and sending transaction data.
"""
import json
import time
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.models.transaction import Transaction
from src.utils.config_loader import get_config, get_config_value


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionProducer:
    """
    Kafka producer for generating and sending transaction data.
    """
    def __init__(self, bootstrap_servers: Optional[str] = None, topic: Optional[str] = None):
        """
        Initialize the TransactionProducer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (comma-separated)
            topic: Kafka topic to produce messages to
        """
        # Load configuration
        kafka_config = get_config("kafka_config")
        producer_config = kafka_config.get("producer", {})
        
        # Set bootstrap servers and topic
        self.bootstrap_servers = bootstrap_servers or get_config_value(
            "kafka_config", "kafka.bootstrap_servers", "localhost:9092"
        )
        self.topic = topic or get_config_value(
            "kafka_config", "topics.input.transactions", "banking.transactions.raw"
        )
        
        # Configure and create Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks=producer_config.get("acks", "all"),
            retries=producer_config.get("retries", 3),
            batch_size=producer_config.get("batch_size", 16384),
            linger_ms=producer_config.get("linger_ms", 10),
            compression_type=producer_config.get("compression_type", "snappy")
        )
        
        logger.info(f"TransactionProducer initialized with bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"Target topic: {self.topic}")
        
        # Sample data for generation
        self.account_ids = [f"ACC{i:06d}" for i in range(1, 101)]
        self.customer_ids = [f"CUST{i:06d}" for i in range(1, 51)]
        self.transaction_types = ["deposit", "withdrawal", "transfer", "payment", "refund"]
        self.merchants = [
            "Amazon", "Walmart", "Target", "Costco", "Starbucks", "McDonald's", 
            "Shell", "Exxon", "AT&T", "Verizon", "Netflix", "Spotify"
        ]
        self.data_sources = ["mobile_app", "web_banking", "branch", "atm", "merchant_pos"]
        
    def _generate_transaction(self) -> Transaction:
        """
        Generate a random transaction for testing.
        
        Returns:
            A Transaction object with random data
        """
        account_id = random.choice(self.account_ids)
        customer_id = random.choice(self.customer_ids)
        transaction_type = random.choice(self.transaction_types)
        
        # Generate reasonable amount based on transaction type
        if transaction_type == "deposit":
            amount = round(random.uniform(50, 5000), 2)
        elif transaction_type == "withdrawal":
            amount = round(random.uniform(20, 1000), 2)
        elif transaction_type == "transfer":
            amount = round(random.uniform(10, 3000), 2)
        elif transaction_type == "payment":
            amount = round(random.uniform(5, 500), 2)
        else:  # refund
            amount = round(random.uniform(5, 200), 2)
            
        # Generate random timestamp within the last 24 hours
        timestamp = datetime.now() - timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # For payments, add merchant info
        merchant_info = None
        if transaction_type == "payment":
            merchant = random.choice(self.merchants)
            merchant_info = {
                "name": merchant,
                "category": random.choice(["retail", "food", "gas", "services", "entertainment"]),
                "merchant_id": f"M{random.randint(10000, 99999)}"
            }
            
        # Create the transaction
        transaction = Transaction(
            transaction_id=str(uuid.uuid4()),
            account_id=account_id,
            customer_id=customer_id,
            transaction_type=transaction_type,
            amount=amount,
            timestamp=timestamp,
            description=f"{transaction_type.title()} transaction",
            status=random.choice(["pending", "completed", "completed", "completed"]),  # More likely to be completed
            source=random.choice(self.data_sources),
            merchant_info=merchant_info,
            data_source=random.choice(self.data_sources)
        )
        
        return transaction
    
    def send_transaction(self, transaction: Transaction) -> None:
        """
        Send a transaction to the Kafka topic.
        
        Args:
            transaction: Transaction object to send
        """
        # Convert transaction to dictionary
        transaction_dict = transaction.to_dict()
        
        # Generate a message key based on account_id for partitioning
        key = transaction.account_id.encode('utf-8') if transaction.account_id else None
        
        # Send the message
        self.producer.send(
            self.topic,
            key=key,
            value=transaction_dict
        ).add_callback(
            self._on_send_success
        ).add_errback(
            self._on_send_error
        )
    
    def _on_send_success(self, record_metadata):
        """
        Callback for successful message send.
        """
        logger.debug(
            f"Message sent to {record_metadata.topic} "
            f"[partition={record_metadata.partition}, offset={record_metadata.offset}]"
        )
    
    def _on_send_error(self, exc):
        """
        Callback for failed message send.
        """
        logger.error(f"Failed to send message: {exc}")
        
    def close(self):
        """
        Close the Kafka producer.
        """
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
            
    def generate_and_send(self, count: int = 1, delay: float = 0.5) -> None:
        """
        Generate and send a specified number of random transactions.
        
        Args:
            count: Number of transactions to generate and send
            delay: Delay between messages in seconds
        """
        logger.info(f"Generating and sending {count} transactions...")
        
        for i in range(count):
            transaction = self._generate_transaction()
            self.send_transaction(transaction)
            
            logger.info(f"Sent transaction {i+1}/{count}: {transaction.transaction_id}")
            
            if i < count - 1 and delay > 0:
                time.sleep(delay)
                
        # Ensure all messages are sent
        self.producer.flush()
        logger.info(f"Successfully sent {count} transactions")


if __name__ == "__main__":
    # Example usage
    producer = TransactionProducer()
    try:
        # Generate and send 10 transactions with a 1-second delay between them
        producer.generate_and_send(count=10, delay=1.0)
    finally:
        producer.close() 