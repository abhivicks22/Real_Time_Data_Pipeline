"""
Transaction validator using Great Expectations.
"""
import os
import logging
import json
import datetime
import pandas as pd
from typing import Dict, Any, List, Optional, Union

import great_expectations as ge
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.dataset import PandasDataset

from src.models.transaction import Transaction
from src.utils.config_loader import get_config, get_config_value


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionValidator:
    """
    Validate transaction data using Great Expectations.
    """
    def __init__(self, context_root_dir: Optional[str] = None):
        """
        Initialize the TransactionValidator.
        
        Args:
            context_root_dir: Root directory for Great Expectations context
        """
        # Load validation configuration
        validation_config = get_config("validation_config")
        ge_config = validation_config.get("great_expectations", {})
        
        # Set context root directory
        self.context_root_dir = context_root_dir or ge_config.get(
            "context_root_dir", "data/great_expectations"
        )
        
        # Set validation results directory
        self.validation_results_dir = ge_config.get(
            "validation_results_dir", "data/great_expectations/validations"
        )
        
        # Ensure directories exist
        os.makedirs(self.context_root_dir, exist_ok=True)
        os.makedirs(self.validation_results_dir, exist_ok=True)
        
        # Create expectation suite for transactions
        self.transaction_suite = self._create_transaction_expectations()
        
        logger.info(f"TransactionValidator initialized with context_root_dir: {self.context_root_dir}")
        
    def _create_transaction_expectations(self) -> ExpectationSuite:
        """
        Create an expectation suite for transaction data.
        
        Returns:
            ExpectationSuite object with expectations for transaction data
        """
        # Create a new expectation suite
        suite = ExpectationSuite(expectation_suite_name="transaction_expectations")
        
        # Add expectations
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "transaction_id"}
            )
        )
        
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "transaction_id"}
            )
        )
        
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "account_id"}
            )
        )
        
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "transaction_type"}
            )
        )
        
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "transaction_type", 
                    "value_set": ["deposit", "withdrawal", "transfer", "payment", "refund"]
                }
            )
        )
        
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "amount",
                    "min_value": 0.01,
                    "max_value": 1000000.00
                }
            )
        )
        
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "currency",
                    "value_set": ["USD", "EUR", "GBP", "JPY", "CAD", "AUD"]
                }
            )
        )
        
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "status",
                    "value_set": ["pending", "completed", "failed", "rejected"]
                }
            )
        )
        
        return suite
    
    def validate_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """
        Validate a single transaction against expectations.
        
        Args:
            transaction: Transaction object to validate
            
        Returns:
            Dictionary with validation results
        """
        # Convert transaction to dictionary and create a DataFrame
        transaction_dict = transaction.to_dict()
        df = pd.DataFrame([transaction_dict])
        
        # Create a Great Expectations dataset
        dataset = PandasDataset(df, expectation_suite=self.transaction_suite)
        
        # Validate the dataset
        validation_result = dataset.validate(result_format="COMPLETE")
        
        # Store validation details in the transaction
        transaction.validation_status = "valid" if validation_result.success else "invalid"
        transaction.validation_errors = []
        
        if not validation_result.success:
            # Extract failed expectations
            for result in validation_result.results:
                if not result.success:
                    error_message = (
                        f"Failed {result.expectation_config.expectation_type}: "
                        f"{result.expectation_config.kwargs}"
                    )
                    transaction.validation_errors.append(error_message)
        
        # Return validation result
        return {
            "transaction_id": transaction.transaction_id,
            "success": validation_result.success,
            "validation_status": transaction.validation_status,
            "validation_errors": transaction.validation_errors,
            "timestamp": datetime.datetime.now().isoformat(),
            "full_result": validation_result.to_json_dict()
        }
    
    def validate_transactions(self, transactions: List[Transaction]) -> Dict[str, Any]:
        """
        Validate multiple transactions against expectations.
        
        Args:
            transactions: List of Transaction objects to validate
            
        Returns:
            Dictionary with validation results summary
        """
        results = []
        valid_count = 0
        invalid_count = 0
        
        for transaction in transactions:
            result = self.validate_transaction(transaction)
            results.append(result)
            
            if result["success"]:
                valid_count += 1
            else:
                invalid_count += 1
        
        # Create summary
        summary = {
            "total_transactions": len(transactions),
            "valid_transactions": valid_count,
            "invalid_transactions": invalid_count,
            "success_rate": valid_count / len(transactions) if transactions else 0,
            "timestamp": datetime.datetime.now().isoformat(),
            "results": results
        }
        
        # Log summary
        logger.info(
            f"Validated {summary['total_transactions']} transactions: "
            f"{summary['valid_transactions']} valid, {summary['invalid_transactions']} invalid"
        )
        
        return summary
    
    def save_validation_results(self, results: Dict[str, Any], filename: Optional[str] = None) -> str:
        """
        Save validation results to a file.
        
        Args:
            results: Validation results to save
            filename: Name of the file to save results to (optional)
            
        Returns:
            Path to the saved results file
        """
        if filename is None:
            # Generate a filename based on current timestamp
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"validation_results_{timestamp}.json"
        
        # Ensure the filename has a .json extension
        if not filename.endswith('.json'):
            filename += '.json'
        
        # Create the full path
        file_path = os.path.join(self.validation_results_dir, filename)
        
        # Save the results
        with open(file_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Validation results saved to: {file_path}")
        return file_path


def validate_transactions_batch(transactions: List[Transaction]) -> Dict[str, Any]:
    """
    Helper function to validate a batch of transactions.
    
    Args:
        transactions: List of Transaction objects to validate
        
    Returns:
        Dictionary with validation results summary
    """
    validator = TransactionValidator()
    return validator.validate_transactions(transactions)


if __name__ == "__main__":
    # Example usage
    from src.kafka.transaction_producer import TransactionProducer
    
    # Generate some sample transactions
    producer = TransactionProducer()
    sample_transactions = [producer._generate_transaction() for _ in range(10)]
    
    # Validate them
    validator = TransactionValidator()
    results = validator.validate_transactions(sample_transactions)
    validator.save_validation_results(results) 