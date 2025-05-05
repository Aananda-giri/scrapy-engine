"""
Parquet storage implementation for OSCAR data processor.

This provides better compression and faster read/write than CSV,
but all data needs to fit in memory for updates.
"""
import logging
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from oscar_processor.storage.base import BaseStorage

logger = logging.getLogger(__name__)


class ParquetStorage(BaseStorage):
    """Parquet-based storage for OSCAR data."""
    
    def __init__(self, output_path: str, filename: str = "oscar_data.parquet"):
        """
        Initialize Parquet storage.
        
        Args:
            output_path: Directory to save Parquet file
            filename: Name of the Parquet file
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True, parents=True)
        self.file_path = self.output_path / filename
        self.csv_index = {}  # URL -> record mapping (for API compatibility)
        self._dataframe = None  # In-memory dataframe
        self.keep_csv_index = str(os.getenv("KEEP_CSV_INDEX", "False")).lower() == "true"
        logger.info(f"Keep CSV index: {self.keep_csv_index}")

    def initialize(self) -> None:
        """Initialize Parquet file if needed."""
        if self.file_path.exists():
            try:
                # Load existing data into memory
                self._dataframe = pd.read_parquet(self.file_path)
                logger.info(f"Loaded existing Parquet file: {self.file_path}")
            except Exception as e:
                logger.error(f"Error loading Parquet file: {e}")
                # Create new dataframe
                self._create_empty_dataframe()
        else:
            # Create new dataframe
            self._create_empty_dataframe()
        if self.keep_csv_index:
            # Build URL index
            self.csv_index = self.build_index()
    
    def _create_empty_dataframe(self):
        """Create an empty DataFrame with the required columns."""
        self._dataframe = pd.DataFrame({
            'content': [],
            'warc_target_uri': [],
            'warc_date': [],
            'content_type': []
        })
        logger.info("Created new empty DataFrame for Parquet storage")
    
    def build_index(self) -> Dict[str, Dict[str, Any]]:
        """
        Build an index of URLs from the DataFrame.
        
        Returns:
            Dictionary mapping URLs to record data
        """
        index = {}
        
        if self._dataframe is None or self._dataframe.empty:
            return index
        
        logger.info("Building index from Parquet data")
        try:
            # Reset the index to get numeric row indexes
            df = self._dataframe.reset_index(drop=True)
            
            for i, row in df.iterrows():
                try:
                    url = row['warc_target_uri']
                    index[url] = {
                        'row_idx': i,
                        'content': row['content'],
                        'warc_date': row['warc_date'],
                        'content_type': row['content_type']
                    }
                except Exception as e:
                    logger.error(f"Error indexing row {i}: {e}")
                    continue
            
            logger.info(f"Loaded {len(index)} entries into index")
        except Exception as e:
            logger.error(f"Error building Parquet index: {e}")
        
        return index
    
    def get_record(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a record by URL.
        
        Args:
            url: URL to look up
            
        Returns:
            Record dictionary if found, None otherwise
        """
        return self.csv_index.get(url)
    
    def save_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Save a batch of new records to the DataFrame.
        
        Args:
            batch: List of dictionaries containing record data
        """
        if not batch:
            return
            
        try:
            # Convert batch to DataFrame
            batch_df = pd.DataFrame(batch)
            
            # Append to existing DataFrame
            self._dataframe = pd.concat([self._dataframe, batch_df], ignore_index=True)
            
            # Save to file
            self._save_to_file()
            
            if self.keep_csv_index:
                # Update index
                for item in batch:
                    url = item['warc_target_uri']
                    self.csv_index[url] = {
                        'row_idx': len(self.csv_index),
                        'content': item['content'],
                        'warc_date': item['warc_date'],
                        'content_type': item['content_type']
                    }
            
            logger.debug(f"Saved {len(batch)} new records to Parquet")
            
        except Exception as e:
            logger.error(f"Error saving batch to Parquet: {e}")
    
    def update_records(self, updates: List[Dict[str, Any]]) -> None:
        """
        Update existing records in the DataFrame.
        
        Args:
            updates: List of dictionaries containing updated record data
        """
        if not updates or self._dataframe is None:
            return
            
        logger.info(f"Updating {len(updates)} records with better scores")
        
        try:
            # Create a URL-based index in the dataframe for faster updates
            # This assumes warc_target_uri is unique
            self._dataframe.set_index('warc_target_uri', inplace=True, drop=False)
            
            for update in updates:
                url = update['warc_target_uri']
                
                if url in self._dataframe.index:
                    self._dataframe.at[url, 'content'] = update['content']
                    self._dataframe.at[url, 'warc_date'] = update['warc_date']
                    self._dataframe.at[url, 'content_type'] = update['content_type']
                    
                    if self.keep_csv_index:
                        # Update memory index
                        self.csv_index[url]['content'] = update['content']
                        self.csv_index[url]['warc_date'] = update['warc_date']
                        self.csv_index[url]['content_type'] = update['content_type']
                    
                    if 'old_score' in update and 'new_score' in update:
                        logger.info(f"Updated URL: {url} - Score: {update['old_score']} -> {update['new_score']}")
            
            # Reset index after updates
            self._dataframe.reset_index(drop=True, inplace=True)
            
            # Save to file
            self._save_to_file()
                
        except Exception as e:
            logger.error(f"Error updating records in Parquet: {e}")
            # Reset index in case of error
            if self._dataframe is not None:
                try:
                    self._dataframe.reset_index(drop=True, inplace=True)
                except:
                    pass
    
    def _save_to_file(self):
        """Save DataFrame to Parquet file."""
        try:
            # Create a temporary file
            temp_file = self.file_path.with_suffix('.tmp')
            
            # Save to temporary file first
            self._dataframe.to_parquet(
                temp_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Replace the original file
            os.replace(temp_file, self.file_path)
            
            logger.debug(f"Saved DataFrame with {len(self._dataframe)} records to {self.file_path}")
            
        except Exception as e:
            logger.error(f"Error saving DataFrame to Parquet: {e}")
            # Clean up temporary file if it exists
            if Path(temp_file).exists():
                os.remove(temp_file)
    
    def close(self) -> None:
        """Save any pending changes and release resources."""
        if self._dataframe is not None:
            try:
                self._save_to_file()
                logger.debug("Parquet storage closed")
            except Exception as e:
                logger.error(f"Error closing Parquet storage: {e}")
    
    def export_to_csv(self, csv_path: str) -> None:
        """
        Export DataFrame to a CSV file.
        
        Args:
            csv_path: Path to CSV file
        """
        try:
            logger.info(f"Exporting Parquet data to CSV: {csv_path}")
            
            if self._dataframe is not None and not self._dataframe.empty:
                self._dataframe.to_csv(csv_path, index=False)
                logger.info(f"Successfully exported {len(self._dataframe)} records to {csv_path}")
            else:
                logger.warning("No data to export to CSV")
                
        except Exception as e:
            logger.error(f"Error exporting Parquet data to CSV: {e}")