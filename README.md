# Real-Time Banking Transaction ETL Pipeline

This project implements a real-time ETL (Extract, Transform, Load) pipeline for processing retail banking transaction data. The system ingests data from multiple sources, processes it in real-time, resolves discrepancies, tracks data lineage, validates the data quality, and stores it for analysis.

## Architecture

![Architecture Diagram](docs/architecture.png)

The pipeline consists of the following components:

1. **Data Ingestion** (Apache Kafka)
   - Consumes transaction data from multiple sources in real-time
   - Provides fault-tolerance and scalability for data ingestion
   - Maintains data ordering and delivery guarantees

2. **Data Processing** (Apache Spark Streaming)
   - Processes transaction data in micro-batches or continuous streams
   - Performs real-time transformations and enrichment
   - Handles late-arriving data and out-of-order events

3. **Data Quality & Validation** (Great Expectations)
   - Validates data against predefined expectations
   - Ensures data integrity and consistency
   - Generates data quality reports

4. **Data Storage** (Apache Hive/HDFS)
   - Stores processed data in a data warehouse structure
   - Provides analytical capabilities for downstream applications
   - Manages data partitioning and optimization

5. **Data Lineage & Metadata** (Custom PostgreSQL)
   - Tracks data sources and transformations
   - Maintains metadata about processed data
   - Enables audit and compliance requirements

## Project Structure

```
real_time_pipeline/
├── src/                     # Source code for the pipeline
│   ├── kafka/               # Kafka producers and consumers
│   ├── spark/               # Spark streaming and batch jobs
│   ├── validation/          # Great Expectations configurations
│   ├── utils/               # Utility functions and helpers
│   └── config/              # Configuration files
├── data/                    # Data directories
│   ├── raw/                 # Raw data landing zone
│   ├── processed/           # Processed data storage
│   └── warehouse/           # Data warehouse storage
├── docs/                    # Documentation
├── tests/                   # Test suite
├── scripts/                 # Deployment and utility scripts
└── requirements.txt         # Python dependencies
```

## Setup and Installation

1. Clone this repository
2. Install the dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Configure your environment (see Configuration section)
4. Start the components (see Running the Pipeline section)

## Configuration

Configuration files are stored in the `src/config` directory:
- `kafka_config.yml`: Kafka broker and topic configurations
- `spark_config.yml`: Spark cluster and job configurations
- `validation_config.yml`: Great Expectations configurations
- `storage_config.yml`: Hive and HDFS configurations

## Running the Pipeline

1. Start Kafka and create the required topics:
   ```
   scripts/start_kafka.sh
   ```

2. Launch the Spark streaming jobs:
   ```
   scripts/start_spark_streaming.sh
   ```

3. Initialize the data validation framework:
   ```
   scripts/init_validation.sh
   ```

4. Start the monitoring dashboard:
   ```
   scripts/start_monitoring.sh
   ```

## Data Models

The pipeline processes several data models:
- Transaction records
- Account information
- Customer data
- Reconciliation data

## Monitoring and Alerting

The pipeline includes monitoring for:
- Data throughput and latency
- Processing errors and exceptions
- Data quality metrics
- System resource utilization.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
