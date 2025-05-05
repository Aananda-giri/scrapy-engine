"""
Base storage interface for OSCAR data processor.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional


class BaseStorage(ABC):
    """Abstract base class for storage implementations."""

    @abstractmethod
    def initialize(self) -> None:
        """Initialize the storage (create files, tables, etc.)."""
        pass

    @abstractmethod
    def save_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Save a batch of records to storage.
        
        Args:
            batch: List of dictionaries containing record data with the following fields:
                - content: The text content of the record
                - warc_target_uri: URL identifier for the record
                - warc_date: Date when the record was created/retrieved
                - content_type: MIME type of the content
                - score: Quality score of the content (float)
        """
        pass

    @abstractmethod
    def update_records(self, updates: List[Dict[str, Any]]) -> None:
        """
        Update existing records in storage.
        
        Args:
            updates: List of dictionaries containing updated record data with the following fields:
                - content: The updated text content
                - warc_target_uri: URL identifier for the record to update
                - warc_date: Updated date
                - content_type: Updated MIME type
                - old_score: Previous quality score (optional)
                - new_score: New quality score (optional)
                - score: Quality score if old/new not provided
        """
        pass

    @abstractmethod
    def get_record(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a record by URL.
        
        Args:
            url: URL to look up
            
        Returns:
            Record dictionary if found with the following fields:
                - content: The text content of the record
                - warc_target_uri: URL identifier
                - warc_date: Date when the record was created/retrieved
                - content_type: MIME type of the content
                - score: Quality score of the content (float)
            Returns None if record not found
        """
        pass

    @abstractmethod
    def build_index(self) -> Dict[str, Dict[str, Any]]:
        """
        Build an index of all records in storage.
        
        Returns:
            Dictionary mapping URLs to record data
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close any open resources."""
        pass

    def __enter__(self):
        """Support context manager protocol."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close resources when exiting context."""
        self.close()