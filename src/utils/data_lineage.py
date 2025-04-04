"""
Utility for tracking data lineage throughout the ETL pipeline.
"""
import os
import json
import logging
import datetime
from typing import Dict, Any, List, Optional, Union
import uuid

import psycopg2
from psycopg2.extras import Json, DictCursor
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from src.utils.config_loader import get_config


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Define SQLAlchemy base
Base = declarative_base()


class DataLineageTracker:
    """
    Track data lineage throughout the ETL pipeline.
    """
    def __init__(self, db_url: Optional[str] = None):
        """
        Initialize the DataLineageTracker.
        
        Args:
            db_url: Database connection URL (optional)
                   If not provided, will use in-memory SQLite
        """
        self.db_url = db_url or "sqlite:///data/lineage.db"
        
        # Create engine and session
        self.engine = create_engine(self.db_url)
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        
        # Initialize database
        self._init_db()
        
        logger.info(f"DataLineageTracker initialized with db_url: {self.db_url}")
        
    def _init_db(self) -> None:
        """
        Initialize the database schema.
        """
        # Define data lineage table dynamically
        self.lineage_table = Table(
            'data_lineage', 
            self.metadata,
            Column('lineage_id', String, primary_key=True),
            Column('transaction_id', String, nullable=False, index=True),
            Column('source_system', String),
            Column('source_timestamp', DateTime),
            Column('processing_stage', String),
            Column('processing_timestamp', DateTime),
            Column('process_name', String),
            Column('process_version', String),
            Column('input_record_hash', String),
            Column('output_record_hash', String),
            Column('status', String),
            Column('metadata', String)  # JSON stored as string
        )
        
        # Create tables if they don't exist
        self.metadata.create_all(self.engine)
        logger.info("Database schema initialized")
        
    def track_lineage(
        self,
        transaction_id: str,
        source_system: str,
        source_timestamp: datetime.datetime,
        processing_stage: str,
        process_name: str,
        process_version: str = "1.0",
        input_data: Optional[Dict[str, Any]] = None,
        output_data: Optional[Dict[str, Any]] = None,
        status: str = "success",
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Track data lineage for a transaction.
        
        Args:
            transaction_id: ID of the transaction
            source_system: System that originated the data
            source_timestamp: Timestamp when data was created in source system
            processing_stage: Current processing stage (e.g., "ingest", "validate", "transform")
            process_name: Name of the process (e.g., "KafkaConsumer", "TransactionValidator")
            process_version: Version of the process
            input_data: Input data for the process
            output_data: Output data from the process
            status: Status of the processing (e.g., "success", "failure")
            metadata: Additional metadata
            
        Returns:
            Lineage ID
        """
        # Generate a lineage ID
        lineage_id = str(uuid.uuid4())
        
        # Calculate hashes for input and output data (if provided)
        input_record_hash = self._calculate_hash(input_data) if input_data else None
        output_record_hash = self._calculate_hash(output_data) if output_data else None
        
        # Create session
        session = self.Session()
        
        try:
            # Insert lineage record
            session.execute(
                self.lineage_table.insert().values(
                    lineage_id=lineage_id,
                    transaction_id=transaction_id,
                    source_system=source_system,
                    source_timestamp=source_timestamp,
                    processing_stage=processing_stage,
                    processing_timestamp=datetime.datetime.now(),
                    process_name=process_name,
                    process_version=process_version,
                    input_record_hash=input_record_hash,
                    output_record_hash=output_record_hash,
                    status=status,
                    metadata=json.dumps(metadata or {})
                )
            )
            
            # Commit the transaction
            session.commit()
            
            logger.debug(
                f"Tracked lineage for transaction {transaction_id} "
                f"at stage {processing_stage} with status {status}"
            )
            
            return lineage_id
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error tracking lineage: {str(e)}", exc_info=True)
            raise
        finally:
            session.close()
            
    def get_transaction_lineage(self, transaction_id: str) -> List[Dict[str, Any]]:
        """
        Get lineage records for a transaction.
        
        Args:
            transaction_id: ID of the transaction
            
        Returns:
            List of lineage records for the transaction
        """
        # Create session
        session = self.Session()
        
        try:
            # Query lineage records
            result = session.execute(
                self.lineage_table.select().where(
                    self.lineage_table.c.transaction_id == transaction_id
                ).order_by(
                    self.lineage_table.c.processing_timestamp
                )
            )
            
            # Convert result to list of dictionaries
            lineage_records = []
            for row in result:
                record = dict(row)
                
                # Parse JSON metadata
                if record.get('metadata'):
                    record['metadata'] = json.loads(record['metadata'])
                    
                lineage_records.append(record)
                
            return lineage_records
            
        except Exception as e:
            logger.error(f"Error retrieving lineage: {str(e)}", exc_info=True)
            return []
        finally:
            session.close()
            
    def _calculate_hash(self, data: Dict[str, Any]) -> str:
        """
        Calculate a hash for data object.
        
        Args:
            data: Data to hash
            
        Returns:
            Hash string
        """
        import hashlib
        
        # Convert to JSON string with sorted keys for consistency
        json_str = json.dumps(data, sort_keys=True, default=str)
        
        # Calculate SHA-256 hash
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


# Create a singleton instance
lineage_tracker = DataLineageTracker()


def track_processing_step(
    transaction_id: str,
    source_system: str,
    source_timestamp: datetime.datetime,
    step_name: str,
    input_data: Optional[Dict[str, Any]] = None,
    output_data: Optional[Dict[str, Any]] = None,
    status: str = "success",
    metadata: Optional[Dict[str, Any]] = None
) -> str:
    """
    Helper function to track a processing step using the singleton lineage tracker.
    
    Args:
        transaction_id: ID of the transaction
        source_system: System that originated the data
        source_timestamp: Timestamp when data was created in source system
        step_name: Name of the processing step
        input_data: Input data for the step
        output_data: Output data from the step
        status: Status of the processing (e.g., "success", "failure")
        metadata: Additional metadata
        
    Returns:
        Lineage ID
    """
    return lineage_tracker.track_lineage(
        transaction_id=transaction_id,
        source_system=source_system,
        source_timestamp=source_timestamp,
        processing_stage=step_name,
        process_name=step_name,
        input_data=input_data,
        output_data=output_data,
        status=status,
        metadata=metadata
    )


if __name__ == "__main__":
    # Example usage
    import random
    from datetime import datetime, timedelta
    
    # Create sample transaction data
    transaction_id = f"TX-{uuid.uuid4()}"
    source_timestamp = datetime.now() - timedelta(minutes=30)
    
    # Track ingest step
    input_data = {
        "transaction_id": transaction_id,
        "amount": 123.45,
        "type": "payment"
    }
    
    output_data = {
        "transaction_id": transaction_id,
        "amount": 123.45,
        "type": "payment",
        "processed": True
    }
    
    # Track multiple steps in the pipeline
    lineage_id_1 = track_processing_step(
        transaction_id=transaction_id,
        source_system="source_bank",
        source_timestamp=source_timestamp,
        step_name="ingest",
        input_data=input_data,
        output_data=output_data,
        metadata={"source_topic": "banking.transactions.raw"}
    )
    
    lineage_id_2 = track_processing_step(
        transaction_id=transaction_id,
        source_system="kafka",
        source_timestamp=datetime.now() - timedelta(minutes=25),
        step_name="validate",
        input_data=output_data,
        output_data={"validated": True, **output_data},
        metadata={"validation_result": "passed"}
    )
    
    lineage_id_3 = track_processing_step(
        transaction_id=transaction_id,
        source_system="validator",
        source_timestamp=datetime.now() - timedelta(minutes=20),
        step_name="transform",
        input_data={"validated": True, **output_data},
        output_data={"transformed": True, "category": "retail", **output_data},
        metadata={"transformation_rules": ["categorization", "normalization"]}
    )
    
    # Get all lineage for the transaction
    lineage = lineage_tracker.get_transaction_lineage(transaction_id)
    
    print(f"Lineage for transaction {transaction_id}:")
    for i, record in enumerate(lineage, 1):
        print(f"  Step {i}: {record['processing_stage']} ({record['status']})")
        print(f"    Time: {record['processing_timestamp']}")
        print(f"    Process: {record['process_name']} v{record['process_version']}")
        print(f"    Metadata: {record['metadata']}")
        print("") 