#!/usr/bin/env python
"""
Script to run the complete ETL pipeline.
"""
import os
import sys
import time
import argparse
import logging
import threading
import signal
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import components
from src.kafka.transaction_producer import TransactionProducer
from src.kafka.transaction_consumer import TransactionConsumer
from src.spark.transaction_processor import TransactionStreamProcessor
from src.validation.transaction_validator import TransactionValidator
from src.models.transaction import Transaction
from src.utils.data_lineage import track_processing_step
from src.utils.config_loader import get_config, get_config_value


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    """Handle termination signals."""
    global shutdown_requested
    logger.info("Shutdown signal received. Stopping pipeline...")
    shutdown_requested = True


def process_transaction(transaction: Transaction):
    """
    Process a transaction through validation and lineage tracking.
    
    Args:
        transaction: Transaction object to process
    """
    try:
        # Track the receipt of the transaction
        track_processing_step(
            transaction_id=transaction.transaction_id,
            source_system=transaction.source,
            source_timestamp=transaction.timestamp,
            step_name="receive",
            input_data=transaction.to_dict(),
            metadata={"topic": "banking.transactions.raw"}
        )
        
        # Validate the transaction
        validator = TransactionValidator()
        validation_result = validator.validate_transaction(transaction)
        
        # Track the validation step
        track_processing_step(
            transaction_id=transaction.transaction_id,
            source_system="consumer",
            source_timestamp=transaction.timestamp,
            step_name="validate",
            input_data=transaction.to_dict(),
            output_data={
                "validation_status": validation_result["validation_status"],
                "validation_errors": validation_result["validation_errors"],
                **transaction.to_dict()
            },
            status=validation_result["success"] and "success" or "failure",
            metadata={"validation_details": validation_result}
        )
        
        # Log the result
        if validation_result["success"]:
            logger.info(f"Transaction {transaction.transaction_id} validated successfully")
        else:
            logger.warning(
                f"Transaction {transaction.transaction_id} validation failed: "
                f"{validation_result['validation_errors']}"
            )
            
    except Exception as e:
        logger.error(f"Error processing transaction {transaction.transaction_id}: {str(e)}", exc_info=True)


def run_producer(args):
    """
    Run the transaction producer.
    
    Args:
        args: Command-line arguments
    """
    logger.info("Starting transaction producer...")
    
    producer = TransactionProducer()
    try:
        # Generate and send transactions
        while not shutdown_requested:
            count = args.batch_size
            delay = args.delay
            
            logger.info(f"Generating {count} transactions with {delay}s delay...")
            producer.generate_and_send(count=count, delay=delay)
            
            # Wait between batches
            if not shutdown_requested:
                logger.info(f"Waiting {args.interval}s until next batch...")
                for _ in range(args.interval):
                    if shutdown_requested:
                        break
                    time.sleep(1)
                    
    except KeyboardInterrupt:
        logger.info("Producer interrupted. Shutting down...")
    finally:
        producer.close()
        logger.info("Producer stopped")


def run_consumer(args):
    """
    Run the transaction consumer.
    
    Args:
        args: Command-line arguments
    """
    logger.info("Starting transaction consumer...")
    
    consumer = TransactionConsumer()
    
    # Add processors
    consumer.add_processor(process_transaction)
    
    try:
        # Start consuming messages
        consumer.start_consuming()
    except KeyboardInterrupt:
        logger.info("Consumer interrupted. Shutting down...")
    finally:
        consumer.close()
        logger.info("Consumer stopped")


def run_spark_processor(args):
    """
    Run the Spark streaming processor.
    
    Args:
        args: Command-line arguments
    """
    logger.info("Starting Spark streaming processor...")
    
    processor = TransactionStreamProcessor()
    
    try:
        # Start the streaming job
        processor.start_streaming()
    except KeyboardInterrupt:
        logger.info("Spark processor interrupted. Shutting down...")
    except Exception as e:
        logger.error(f"Error in Spark processor: {str(e)}", exc_info=True)
    finally:
        logger.info("Spark processor stopped")


def run_pipeline(args):
    """
    Run the complete ETL pipeline with all components.
    
    Args:
        args: Command-line arguments
    """
    logger.info("Starting the complete ETL pipeline...")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create thread for producer
    producer_thread = threading.Thread(target=run_producer, args=(args,))
    producer_thread.daemon = True
    
    # Create thread for consumer
    consumer_thread = threading.Thread(target=run_consumer, args=(args,))
    consumer_thread.daemon = True
    
    # Start threads
    producer_thread.start()
    consumer_thread.start()
    
    if args.spark:
        # Run Spark processor in main thread
        run_spark_processor(args)
    else:
        # Wait for threads to complete
        try:
            while producer_thread.is_alive() or consumer_thread.is_alive():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Pipeline interrupted. Shutting down...")
            global shutdown_requested
            shutdown_requested = True
            
    logger.info("Pipeline execution completed")


def parse_args():
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run the banking transactions ETL pipeline")
    
    parser.add_argument(
        "--mode", 
        choices=["producer", "consumer", "spark", "pipeline"],
        default="pipeline",
        help="Which component(s) to run (default: pipeline)"
    )
    
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=10,
        help="Number of transactions to generate in each batch (default: 10)"
    )
    
    parser.add_argument(
        "--delay", 
        type=float, 
        default=0.5,
        help="Delay between transactions in seconds (default: 0.5)"
    )
    
    parser.add_argument(
        "--interval", 
        type=int, 
        default=5,
        help="Interval between batches in seconds (default: 5)"
    )
    
    parser.add_argument(
        "--spark", 
        action="store_true",
        help="Run Spark streaming processor"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    # Ensure log directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Parse arguments
    args = parse_args()
    
    # Run the selected component(s)
    if args.mode == "producer":
        run_producer(args)
    elif args.mode == "consumer":
        run_consumer(args)
    elif args.mode == "spark":
        run_spark_processor(args)
    else:  # pipeline
        run_pipeline(args)


if __name__ == "__main__":
    main() 