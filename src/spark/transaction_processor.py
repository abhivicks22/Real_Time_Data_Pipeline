"""
Spark streaming processor for real-time transaction processing.
"""
import os
import logging
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum, avg, 
    explode, expr, when, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, ArrayType, MapType, BooleanType
)

from src.utils.config_loader import get_config, get_config_value


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionStreamProcessor:
    """
    Process transaction data using Spark Streaming.
    """
    def __init__(self):
        """
        Initialize the Spark streaming processor.
        """
        # Load configurations
        self.spark_config = get_config("spark_config")
        self.kafka_config = get_config("kafka_config")
        
        # Extract Spark configuration values
        app_name = get_config_value("spark_config", "spark.app_name", "BankingTransactionETL")
        master = get_config_value("spark_config", "spark.master", "local[*]")
        
        # Extract streaming configuration
        self.checkpoint_dir = get_config_value(
            "spark_config", "streaming.checkpoint_dir", "data/checkpoints"
        )
        self.batch_interval = get_config_value(
            "spark_config", "streaming.batch_interval", 10
        )
        self.output_mode = get_config_value(
            "spark_config", "streaming.output_mode", "append"
        )
        self.trigger_type = get_config_value(
            "spark_config", "streaming.trigger_type", "ProcessingTime"
        )
        self.trigger_interval = get_config_value(
            "spark_config", "streaming.trigger_interval", "10 seconds"
        )
        
        # Kafka configuration
        self.bootstrap_servers = get_config_value(
            "kafka_config", "kafka.bootstrap_servers", "localhost:9092"
        )
        self.input_topic = get_config_value(
            "kafka_config", "topics.input.transactions", "banking.transactions.raw"
        )
        self.output_topic = get_config_value(
            "kafka_config", "topics.output.enriched_transactions", "banking.transactions.enriched"
        )
        
        # Create Spark session
        self.spark = self._create_spark_session(app_name, master)
        
        # Create schema for transaction data
        self.transaction_schema = self._create_transaction_schema()
        
        logger.info(f"TransactionStreamProcessor initialized with app_name: {app_name}")
        
    def _create_spark_session(self, app_name: str, master: str) -> SparkSession:
        """
        Create and configure a Spark session.
        
        Args:
            app_name: Name of the Spark application
            master: Spark master URL
            
        Returns:
            Configured SparkSession
        """
        # Extract Spark configuration
        executor_memory = get_config_value("spark_config", "executor.memory", "2g")
        executor_instances = get_config_value("spark_config", "executor.instances", 2)
        executor_cores = get_config_value("spark_config", "executor.cores", 2)
        driver_memory = get_config_value("spark_config", "driver.memory", "2g")
        
        # Create the Spark session builder
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.executor.instances", executor_instances) \
            .config("spark.executor.cores", executor_cores) \
            .config("spark.driver.memory", driver_memory)
        
        # Add custom configurations
        spark_configs = get_config_value("spark_config", "config", {})
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
            
        # Create and return the Spark session
        return builder.getOrCreate()
    
    def _create_transaction_schema(self) -> StructType:
        """
        Create a schema for transaction data.
        
        Returns:
            Spark StructType schema for transaction data
        """
        return StructType([
            StructField("transaction_id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("description", StringType(), True),
            StructField("status", StringType(), True),
            StructField("source", StringType(), True),
            StructField("reference_id", StringType(), True),
            StructField("merchant_info", MapType(StringType(), StringType()), True),
            StructField("location", MapType(StringType(), StringType()), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("processing_timestamp", StringType(), True),
            StructField("validation_status", StringType(), True),
            StructField("validation_errors", ArrayType(StringType()), True),
            StructField("enrichment_status", StringType(), True),
            StructField("data_source", StringType(), True),
            StructField("batch_id", StringType(), True)
        ])
    
    def start_streaming(self) -> None:
        """
        Start the Spark streaming job to process transactions.
        """
        logger.info("Starting Spark streaming job...")
        
        try:
            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.input_topic) \
                .option("startingOffsets", "earliest") \
                .load()
            
            # Parse the JSON data
            parsed_df = df \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .select(
                    col("key"),
                    from_json(col("value"), self.transaction_schema).alias("data")
                ) \
                .select("key", "data.*")
                
            # Convert timestamp strings to timestamp type
            parsed_df = parsed_df \
                .withColumn("event_time", to_timestamp(col("timestamp"))) \
                .withColumn("processing_time", to_timestamp(col("processing_timestamp"))) \
                .withColumn("ingestion_time", current_timestamp())
            
            # Validate data
            validated_df = self._validate_data(parsed_df)
            
            # Enrich data
            enriched_df = self._enrich_data(validated_df)
            
            # Perform windowed aggregations
            aggregated_df = self._perform_aggregations(enriched_df)
            
            # Write the enriched data back to Kafka
            kafka_output = enriched_df \
                .selectExpr(
                    "account_id AS key",
                    "to_json(struct(*)) AS value"
                ) \
                .writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("topic", self.output_topic) \
                .option("checkpointLocation", os.path.join(self.checkpoint_dir, "kafka")) \
                .outputMode(self.output_mode)
            
            # Write aggregated data to console for debugging
            console_output = aggregated_df \
                .writeStream \
                .format("console") \
                .option("truncate", "false") \
                .outputMode("complete") \
                .trigger(processingTime=self.trigger_interval)
                
            # Start the streaming queries
            kafka_query = kafka_output.start()
            console_query = console_output.start()
            
            # Wait for the termination of both queries
            kafka_query.awaitTermination()
            console_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in Spark streaming job: {str(e)}", exc_info=True)
            
    def _validate_data(self, df):
        """
        Validate the transaction data.
        
        Args:
            df: DataFrame containing transaction data
            
        Returns:
            DataFrame with validation columns added
        """
        # Check required fields
        validated_df = df.withColumn(
            "is_valid",
            (col("transaction_id").isNotNull()) &
            (col("account_id").isNotNull()) &
            (col("transaction_type").isNotNull()) &
            (col("amount").isNotNull() & (col("amount") > 0))
        )
        
        # Add validation message
        validated_df = validated_df.withColumn(
            "validation_result",
            when(col("is_valid"), "valid").otherwise("invalid")
        )
        
        return validated_df
    
    def _enrich_data(self, df):
        """
        Enrich the transaction data with additional information.
        
        Args:
            df: DataFrame containing validated transaction data
            
        Returns:
            DataFrame with enrichment columns added
        """
        # Add transaction category based on transaction_type and amount
        enriched_df = df.withColumn(
            "transaction_category",
            when(col("transaction_type") == "deposit", 
                when(col("amount") > 1000, "large_deposit").otherwise("regular_deposit")
            ).when(col("transaction_type") == "withdrawal",
                when(col("amount") > 500, "large_withdrawal").otherwise("regular_withdrawal")
            ).when(col("transaction_type") == "payment",
                when(col("amount") > 100, "large_payment").otherwise("regular_payment")
            ).otherwise(col("transaction_type"))
        )
        
        # In a real application, you'd join with account and customer data here
        
        return enriched_df
    
    def _perform_aggregations(self, df):
        """
        Perform windowed aggregations on the transaction data.
        
        Args:
            df: DataFrame containing enriched transaction data
            
        Returns:
            DataFrame with aggregated results
        """
        # Group by window and account_id
        windowed_df = df \
            .withWatermark("event_time", "30 minutes") \
            .groupBy(
                window(col("event_time"), "1 hour"),
                col("account_id")
            ) \
            .agg(
                count("transaction_id").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount")
            )
            
        return windowed_df


if __name__ == "__main__":
    # Example usage
    processor = TransactionStreamProcessor()
    processor.start_streaming() 