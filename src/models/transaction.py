"""
Transaction model for banking transaction data.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
import json
import uuid


@dataclass
class Transaction:
    """
    Represents a banking transaction with all necessary fields.
    """
    transaction_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    account_id: str = ""
    customer_id: Optional[str] = None
    transaction_type: str = ""  # deposit, withdrawal, transfer, payment, refund
    amount: float = 0.0
    currency: str = "USD"
    timestamp: datetime = field(default_factory=datetime.now)
    description: str = ""
    status: str = "pending"  # pending, completed, failed, rejected
    source: str = ""  # system of origin
    reference_id: Optional[str] = None  # for linking related transactions
    merchant_info: Optional[Dict[str, Any]] = None
    location: Optional[Dict[str, Any]] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Fields used for data lineage and processing
    processing_timestamp: Optional[datetime] = None
    validation_status: Optional[str] = None
    validation_errors: List[str] = field(default_factory=list)
    enrichment_status: Optional[str] = None
    data_source: Optional[str] = None
    batch_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert Transaction object to a dictionary.
        """
        result = {
            "transaction_id": self.transaction_id,
            "account_id": self.account_id,
            "customer_id": self.customer_id,
            "transaction_type": self.transaction_type,
            "amount": self.amount,
            "currency": self.currency,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "description": self.description,
            "status": self.status,
            "source": self.source,
            "reference_id": self.reference_id,
            "merchant_info": self.merchant_info,
            "location": self.location,
            "tags": self.tags,
            "metadata": self.metadata,
            "processing_timestamp": self.processing_timestamp.isoformat() if self.processing_timestamp else None,
            "validation_status": self.validation_status,
            "validation_errors": self.validation_errors,
            "enrichment_status": self.enrichment_status,
            "data_source": self.data_source,
            "batch_id": self.batch_id
        }
        return {k: v for k, v in result.items() if v is not None}
    
    def to_json(self) -> str:
        """
        Convert Transaction object to a JSON string.
        """
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Transaction':
        """
        Create a Transaction object from a dictionary.
        """
        # Handle datetime conversion
        if 'timestamp' in data and isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            
        if 'processing_timestamp' in data and isinstance(data['processing_timestamp'], str):
            data['processing_timestamp'] = datetime.fromisoformat(data['processing_timestamp'].replace('Z', '+00:00'))
            
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Transaction':
        """
        Create a Transaction object from a JSON string.
        """
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def enrich_with_account_info(self, account_info: Dict[str, Any]) -> None:
        """
        Enrich transaction with account information.
        """
        if not self.customer_id and "customer_id" in account_info:
            self.customer_id = account_info["customer_id"]
            
        if "account_type" in account_info:
            self.metadata["account_type"] = account_info["account_type"]
            
        if "account_status" in account_info:
            self.metadata["account_status"] = account_info["account_status"]
        
        self.enrichment_status = "enriched_with_account"
    
    def validate(self) -> bool:
        """
        Validate the transaction data.
        """
        self.validation_errors = []
        
        # Check required fields
        if not self.transaction_id:
            self.validation_errors.append("Missing transaction_id")
            
        if not self.account_id:
            self.validation_errors.append("Missing account_id")
            
        if not self.transaction_type:
            self.validation_errors.append("Missing transaction_type")
        elif self.transaction_type not in ["deposit", "withdrawal", "transfer", "payment", "refund"]:
            self.validation_errors.append(f"Invalid transaction_type: {self.transaction_type}")
            
        if self.amount <= 0:
            self.validation_errors.append(f"Invalid amount: {self.amount}")
            
        # Set validation status
        if not self.validation_errors:
            self.validation_status = "valid"
            return True
        else:
            self.validation_status = "invalid"
            return False 