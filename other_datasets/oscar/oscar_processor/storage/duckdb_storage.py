import logging
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

import duckdb
import pandas as pd

from oscar_processor.storage.base import BaseStorage

logger = logging.getLogger(__name__)


class DuckDBStorage(BaseStorage):
    """DuckDB-based storage for OSCAR data."""
    
    def __init__(self, output_path: str, filename: str = "oscar_data.duckdb"):
        """
        Initialize DuckDB storage.
        
        Args:
            output_path: Directory to save DuckDB file
            filename: Name of the DuckDB file
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True, parents=True)
        self.db_file = self.output_path / filename
        
        self.conn = None
        self.keep_index = str(os.getenv("KEEP_DUCKDB_INDEX", "False")).lower() == "true"
        logger.info(f"Keep DuckDB index: {self.keep_index}")
        self.url_index = {}  # URL -> record mapping
        self.chunk_size = 50000  # For processing large data in chunks
        
    def initialize(self) -> None:
        """Create DuckDB database and table if they don't exist."""
        try:
            self.conn = duckdb.connect(str(self.db_file))
            
            # Create table without explicit ID column - let DuckDB handle it internally
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS oscar_data (
                    content TEXT,
                    warc_target_uri VARCHAR,
                    warc_date VARCHAR,
                    content_type VARCHAR,
                    score DOUBLE
                )
            """)
            
            # Create index on warc_target_uri for faster lookups
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_url ON oscar_data(warc_target_uri)
            """)
            
            if self.keep_index:
                # Build index from existing database
                self.url_index = self.build_index()
                
        except Exception as e:
            logger.error(f"Error initializing DuckDB storage: {e}")
            raise
    
    def build_index(self) -> Dict[str, Dict[str, Any]]:
        """
        Build an index of URLs from the DuckDB database.
        
        Returns:
            Dictionary mapping URLs to record data
        """
        index = {}
        
        try:
            # Check if the table exists and has data
            result = self.conn.execute("SELECT COUNT(*) FROM oscar_data").fetchone()
            if result[0] == 0:
                return index
            
            logger.info("Building URL index from DuckDB database")
            
            # Query all data in chunks for memory efficiency
            cursor = self.conn.cursor()
            cursor.execute("SELECT content, warc_target_uri, warc_date, content_type, score FROM oscar_data")
            
            while True:
                rows = cursor.fetchmany(self.chunk_size)
                if not rows:
                    break
                    
                for row in rows:
                    try:
                        index[row[1]] = {  # row[1] is warc_target_uri
                            'content': row[0],
                            'warc_target_uri': row[1],
                            'warc_date': row[2],
                            'content_type': row[3],
                            'score': row[4]
                        }
                    except Exception as e:
                        logger.error(f"Error indexing row: {e}")
                        continue
            
            logger.info(f"Loaded {len(index)} entries into URL index")
            
        except Exception as e:
            logger.error(f"Error building DuckDB index: {e}")
        
        return index
    
    def get_record(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a record by URL.
        
        Args:
            url: URL to look up
            
        Returns:
            Record dictionary if found, None otherwise
        """
        if self.keep_index:
            # Use in-memory index for faster lookups
            return self.url_index.get(url)
        else:
            # Query the database directly
            try:
                query = "SELECT content, warc_target_uri, warc_date, content_type, score FROM oscar_data WHERE warc_target_uri = ?"
                result = self.conn.execute(query, [url]).fetchone()
                
                if result:
                    return {
                        'content': result[0],
                        'warc_target_uri': result[1],
                        'warc_date': result[2],
                        'content_type': result[3],
                        'score': result[4]
                    }
                    
            except Exception as e:
                logger.error(f"Error retrieving record for URL {url}: {e}")
                raise  # Raise the exception to notify caller
                
            return None
    
    def save_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Save a batch of new records to the DuckDB database.
        
        Args:
            batch: List of dictionaries containing record data
        """
        if not batch:
            logger.warning("Attempted to save empty batch")
            return
            
        try:
            # Log initial batch size
            logger.info(f"Attempting to save batch of {len(batch)} records")
            
            # Check for duplicate URLs in this batch
            # urls = [item['warc_target_uri'] for item in batch]
            # if len(urls) != len(set(urls)):
            #     logger.warning(f"Batch contains {len(urls) - len(set(urls))} duplicate URLs")
            
            # Prepare data for insertion
            df = pd.DataFrame([{
                'content': item['content'],
                'warc_target_uri': item['warc_target_uri'],
                'warc_date': item['warc_date'],
                'content_type': item['content_type'],
                'score': item.get('score', 0.0)  # Default to 0.0 if score is not provided
            } for item in batch])
            
            # # Check if any of these URLs already exist in the database
            # # Get list of existing URLs in this batch
            # if len(urls) > 0:
            #     placeholders = ', '.join(['?' for _ in urls])
            #     query = f"SELECT warc_target_uri FROM oscar_data WHERE warc_target_uri IN ({placeholders})"
            #     existing_urls = [row[0] for row in self.conn.execute(query, urls).fetchall()]
                
            #     if existing_urls:
            #         logger.warning(f"Found {len(existing_urls)} URLs that already exist in the database")
            #         # Filter out already existing URLs from the DataFrame
            #         df = df[~df['warc_target_uri'].isin(existing_urls)]
            
            if df.empty:
                logger.warning("All records in batch already exist in the database")
                return
            
            # Direct insert from DataFrame
            self.conn.execute("BEGIN TRANSACTION")
            row_count = len(df)
            self.conn.execute("INSERT INTO oscar_data SELECT * FROM df")
            self.conn.execute("COMMIT")
            
            logger.info(f"Successfully saved {row_count} new records to DuckDB")
            
            # Update in-memory index if enabled
            if self.keep_index:
                for _, row in df.iterrows():
                    url = row['warc_target_uri']
                    self.url_index[url] = {
                        'content': row['content'],
                        'warc_target_uri': url,
                        'warc_date': row['warc_date'],
                        'content_type': row['content_type'],
                        'score': row['score']
                    }
            
        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"Error saving batch to DuckDB: {e}")
            # Show part of the batch for debugging
            sample = str(batch[:2]) if batch else "Empty batch"
            logger.error(f"Batch sample: {sample}")
            raise  # Raise the exception to notify caller
    
    def update_records(self, updates: List[Dict[str, Any]]) -> None:
        """
        Update existing records in the DuckDB database.
        
        Args:
            updates: List of dictionaries containing updated record data
        """
        if not updates:
            return
            
        logger.info(f"Updating {len(updates)} records with better scores")
        
        try:
            # Process updates in a single transaction for better performance
            self.conn.execute("BEGIN TRANSACTION")
            
            update_count = 0
            for update in updates:
                url = update['warc_target_uri']
                
                # Update record in database
                result = self.conn.execute("""
                    UPDATE oscar_data 
                    SET content = ?, warc_date = ?, content_type = ?, score = ?
                    WHERE warc_target_uri = ?
                """, [
                    update['content'], 
                    update['warc_date'], 
                    update['content_type'],
                    update.get('new_score', update.get('score', 0.0)),  # Use new_score if available, fall back to score
                    url
                ])
                
                # Check if the update was successful
                if result.fetchone()[0] > 0:
                    update_count += 1
                    
                    # Update in-memory index if enabled
                    if self.keep_index and url in self.url_index:
                        self.url_index[url]['content'] = update['content']
                        self.url_index[url]['warc_date'] = update['warc_date']
                        self.url_index[url]['content_type'] = update['content_type']
                        self.url_index[url]['score'] = update.get('new_score', update.get('score', 0.0))
                    
                    if 'old_score' in update and 'new_score' in update:
                        logger.info(f"Updated URL: {url} - Score: {update['old_score']} -> {update['new_score']}")
                else:
                    logger.warning(f"Failed to update URL: {url} - Record not found")
            
            self.conn.execute("COMMIT")
            logger.info(f"Successfully updated {update_count} out of {len(updates)} records")
            
        except Exception as e:
            logger.error(f"Error updating records in DuckDB: {e}")
            self.conn.execute("ROLLBACK")
            raise  # Raise the exception to notify caller
    
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.debug("DuckDB connection closed")
            except Exception as e:
                logger.error(f"Error closing DuckDB connection: {e}")
                raise  # Raise the exception to notify caller