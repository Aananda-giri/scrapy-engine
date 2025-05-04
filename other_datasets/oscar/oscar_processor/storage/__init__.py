"""
Storage module initialization.
"""
import logging
from typing import Optional, Literal

from oscar_processor.storage.base import BaseStorage
from oscar_processor.storage.csv_storage import CSVStorage
from oscar_processor.storage.sqlite_storage import SQLiteStorage
from oscar_processor.storage.parquet_storage import ParquetStorage

logger = logging.getLogger(__name__)


def get_storage(
    storage_type: Literal["csv", "sqlite", "parquet"],
    output_path: str,
    filename: Optional[str] = None
) -> BaseStorage:
    """
    Factory function to get a storage instance based on type.
    
    Args:
        storage_type: Type of storage ("csv", "sqlite", or "parquet")
        output_path: Directory to save data
        filename: Optional filename (will use default if None)
        
    Returns:
        Instance of storage class
        
    Raises:
        ValueError: If invalid storage type is specified
    """
    if storage_type == "csv":
        if filename is None:
            filename = "filtered_oscar_data.csv"
        storage = CSVStorage(output_path=output_path, filename=filename)
    elif storage_type == "sqlite":
        if filename is None:
            filename = "oscar_data.db"
        storage = SQLiteStorage(output_path=output_path, filename=filename)
    elif storage_type == "parquet":
        if filename is None:
            filename = "oscar_data.parquet"
        storage = ParquetStorage(output_path=output_path, filename=filename)
    else:
        raise ValueError(f"Invalid storage type: {storage_type}")
    
    # Initialize storage
    storage.initialize()
    
    logger.info(f"Initialized {storage_type} storage at {output_path}/{filename}")
    return storage