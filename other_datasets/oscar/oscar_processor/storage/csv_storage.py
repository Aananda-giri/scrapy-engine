"""
CSV storage implementation for OSCAR data processor.
"""
import csv
import logging
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Iterator

import pandas as pd

from oscar_processor.storage.base import BaseStorage

logger = logging.getLogger(__name__)


class CSVStorage(BaseStorage):
    """CSV-based storage for OSCAR data."""
    
    def __init__(self, output_path: str, filename: str = "filtered_oscar_data.csv"):
        """
        Initialize CSV storage.
        
        Args:
            output_path: Directory to save CSV file
            filename: Name of the CSV file
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True, parents=True)
        self.output_file = self.output_path / filename
        
        self.keep_csv_index = str(os.getenv("KEEP_CSV_INDEX", "False")).lower() == "true"
        logger.info(f"Keep CSV index: {self.keep_csv_index}")
        self.csv_index = {}  # URL -> record mapping (not efficient for large files)
        self.chunk_size = 100000  # For processing large files in chunks
        
    def initialize(self) -> None:
        """Create CSV file with headers if it doesn't exist."""
        if not self.output_file.exists():
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['content', 'warc_target_uri', 'warc_date', 'content_type', 'score'])
        
        if self.keep_csv_index:
            # Build index from existing CSV
            self.csv_index = self.build_index()
    
    def build_index(self) -> Dict[str, Dict[str, Any]]:
        """
        Build an index of URLs from the CSV file.
        
        Returns:
            Dictionary mapping URLs to record data
        """
        index = {}
        
        if not self.output_file.exists() or self.output_file.stat().st_size == 0:
            return index
        
        logger.info(f"Building index from CSV file: {self.output_file}")
        try:
            # Read the CSV file in chunks to handle large files
            for i, chunk in enumerate(pd.read_csv(self.output_file, chunksize=self.chunk_size)):
                logger.debug(f"Processing chunk {i} with {len(chunk)} rows")
                for _, row in chunk.iterrows():
                    try:
                        # Convert pandas Series to dict for storage
                        index[row['warc_target_uri']] = {
                            'row_idx': len(index),
                            'content': row['content'],
                            'warc_target_uri': row['warc_target_uri'],
                            'warc_date': row['warc_date'],
                            'content_type': row['content_type'],
                            'score': row.get('score', 0.0)  # Default to 0.0 if score column doesn't exist
                        }
                    except Exception as e:
                        logger.error(f"Error indexing row: {e}")
                        continue
            
            logger.info(f"Loaded {len(index)} entries into index")
        except Exception as e:
            logger.error(f"Error building CSV index: {e}")
        
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
        Save a batch of new records to the CSV file.
        
        Args:
            batch: List of dictionaries containing record data
        """
        if not batch:
            return
            
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for item in batch:
                    writer.writerow([
                        item['content'],
                        item['warc_target_uri'],
                        item['warc_date'],
                        item['content_type'],
                        item.get('score', 0.0)  # Include the score field
                    ])
                    if self.keep_csv_index:
                        # Update index
                        self.csv_index[item['warc_target_uri']] = {
                            'row_idx': len(self.csv_index),
                            'content': item['content'],
                            'warc_target_uri': item['warc_target_uri'],
                            'warc_date': item['warc_date'],
                            'content_type': item['content_type'],
                            'score': item.get('score', 0.0)
                        }
            
            logger.debug(f"Saved {len(batch)} new records to CSV")
        except Exception as e:
            logger.error(f"Error saving batch to CSV: {e}")
    
    def update_records(self, updates: List[Dict[str, Any]]) -> None:
        """
        Update existing records in the CSV file.
        
        Args:
            updates: List of dictionaries containing updated record data
        """
        if not updates:
            return
            
        logger.info(f"Updating {len(updates)} records with better scores")
        
        try:
            # For smaller updates or smaller files, use simple read-modify-write approach
            if len(updates) < 100 or len(self.csv_index) < 10000:
                self._update_records_simple(updates)
            else:
                # For larger updates or files, use chunk-based approach
                self._update_records_chunked(updates)
                
        except Exception as e:
            logger.error(f"Error updating records in CSV: {e}")
    
    def _update_records_simple(self, updates: List[Dict[str, Any]]) -> None:
        """
        Simple read-modify-write approach for updating records.
        Better for smaller files or few updates.
        
        Args:
            updates: List of dictionaries containing updated record data
        """
        try:
            df = pd.read_csv(self.output_file)
            
            for update in updates:
                url = update['warc_target_uri']
                mask = df['warc_target_uri'] == url
                
                if mask.any():
                    df.loc[mask, 'content'] = update['content']
                    df.loc[mask, 'warc_date'] = update['warc_date']
                    df.loc[mask, 'content_type'] = update['content_type']
                    # Update score field
                    df.loc[mask, 'score'] = update.get('new_score', update.get('score', 0.0))
                    
                    if self.keep_csv_index:
                        # Update index
                        self.csv_index[url]['content'] = update['content']
                        self.csv_index[url]['warc_date'] = update['warc_date']
                        self.csv_index[url]['content_type'] = update['content_type']
                        self.csv_index[url]['score'] = update.get('new_score', update.get('score', 0.0))
                    
                    if 'old_score' in update and 'new_score' in update:
                        logger.info(f"Updated URL: {url} - Score: {update['old_score']} -> {update['new_score']}")
            
            # Write back to file
            df.to_csv(self.output_file, index=False)
            
        except Exception as e:
            logger.error(f"Error in simple update: {e}")
            raise
    
    def _update_records_chunked(self, updates: List[Dict[str, Any]]) -> None:
        """
        Chunk-based approach for updating records.
        Better for larger files or many updates.
        
        Args:
            updates: List of dictionaries containing updated record data
        """
        try:
            # Create a temporary file to write updated data
            temp_file = self.output_file.with_suffix('.tmp')
            
            # Prepare a lookup dictionary for faster access to updates
            update_lookup = {item['warc_target_uri']: item for item in updates}
            
            # Open output file for writing
            with open(temp_file, 'w', newline='', encoding='utf-8') as out_f:
                writer = csv.writer(out_f)
                writer.writerow(['content', 'warc_target_uri', 'warc_date', 'content_type', 'score'])
                
                # Process input file in chunks
                for chunk in pd.read_csv(self.output_file, chunksize=self.chunk_size):
                    # Identify rows that need updating in this chunk
                    for _, row in chunk.iterrows():
                        url = row['warc_target_uri']
                        
                        if url in update_lookup:
                            # Use updated values
                            update = update_lookup[url]
                            writer.writerow([
                                update['content'],
                                url,
                                update['warc_date'],
                                update['content_type'],
                                update.get('new_score', update.get('score', 0.0))  # Use new_score if available
                            ])
                            
                            if self.keep_csv_index:
                                # Update index
                                self.csv_index[url]['content'] = update['content']
                                self.csv_index[url]['warc_date'] = update['warc_date']
                                self.csv_index[url]['content_type'] = update['content_type']
                                self.csv_index[url]['score'] = update.get('new_score', update.get('score', 0.0))
                            
                            if 'old_score' in update and 'new_score' in update:
                                logger.info(f"Updated URL: {url} - Score: {update['old_score']} -> {update['new_score']}")
                        else:
                            # Use existing values
                            score_value = row.get('score', 0.0)  # Handle cases where score column might not exist
                            writer.writerow([
                                row['content'],
                                url,
                                row['warc_date'],
                                row['content_type'],
                                score_value
                            ])
            
            # Replace original file with temporary file
            os.replace(temp_file, self.output_file)
            
        except Exception as e:
            logger.error(f"Error in chunked update: {e}")
            # Clean up temporary file if it exists
            if Path(temp_file).exists():
                os.remove(temp_file)
            raise
    
    def close(self) -> None:
        """Release resources."""
        # Nothing to close for CSV, but implementation required by BaseStorage
        pass