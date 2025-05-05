"""
DuckDB storage implementation for OSCAR data processor.
"""
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
        self.keep_index = str(os.getenv("KEEP_DUCKDB_INDEX", "True")).lower() == "true"
        logger.info(f"Keep DuckDB index: {self.keep_index}")
        self.url_index = {}  # URL -> record mapping
        self.chunk_size = 100000  # For processing large data in chunks
        
    def initialize(self) -> None:
        """Create DuckDB database and table if they don't exist."""
        try:
            self.conn = duckdb.connect(str(self.db_file))
            
            # Create table if it doesn't exist
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS oscar_data (
                    id INTEGER PRIMARY KEY,
                    content TEXT,
                    warc_target_uri VARCHAR UNIQUE,
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
            cursor.execute("SELECT id, content, warc_target_uri, warc_date, content_type, score FROM oscar_data")
            
            while True:
                rows = cursor.fetchmany(self.chunk_size)
                if not rows:
                    break
                    
                for row in rows:
                    try:
                        index[row[2]] = {  # row[2] is warc_target_uri
                            'row_id': row[0],
                            'content': row[1],
                            'warc_target_uri': row[2],
                            'warc_date': row[3],
                            'content_type': row[4],
                            'score': row[5]
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
                
            return None
    
    def save_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Save a batch of new records to the DuckDB database.
        
        Args:
            batch: List of dictionaries containing record data
        """
        if not batch:
            return
            
        try:
            # Prepare data for insertion
            df = pd.DataFrame([{
                'content': item['content'],
                'warc_target_uri': item['warc_target_uri'],
                'warc_date': item['warc_date'],
                'content_type': item['content_type'],
                'score': item.get('score', 0.0)  # Default to 0.0 if score is not provided
            } for item in batch])
            
            # Insert data using DuckDB's append function (handles transaction internally)
            self.conn.execute("""
                INSERT OR IGNORE INTO oscar_data (content, warc_target_uri, warc_date, content_type, score)
                SELECT content, warc_target_uri, warc_date, content_type, score FROM df
            """)
            
            # Update in-memory index if enabled
            if self.keep_index:
                # Get IDs of newly inserted records
                urls = [item['warc_target_uri'] for item in batch]
                placeholders = ', '.join(['?' for _ in urls])
                query = f"SELECT id, warc_target_uri FROM oscar_data WHERE warc_target_uri IN ({placeholders})"
                results = self.conn.execute(query, urls).fetchall()
                
                # Update index with newly inserted records
                for row_id, url in results:
                    for item in batch:
                        if item['warc_target_uri'] == url:
                            self.url_index[url] = {
                                'row_id': row_id,
                                'content': item['content'],
                                'warc_target_uri': url,
                                'warc_date': item['warc_date'],
                                'content_type': item['content_type'],
                                'score': item.get('score', 0.0)
                            }
                            break
            
            logger.debug(f"Saved {len(batch)} new records to DuckDB")
            
        except Exception as e:
            logger.error(f"Error saving batch to DuckDB: {e}")
    
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
            
            for update in updates:
                url = update['warc_target_uri']
                
                # Update record in database
                self.conn.execute("""
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
                
                # Update in-memory index if enabled
                if self.keep_index and url in self.url_index:
                    self.url_index[url]['content'] = update['content']
                    self.url_index[url]['warc_date'] = update['warc_date']
                    self.url_index[url]['content_type'] = update['content_type']
                    self.url_index[url]['score'] = update.get('new_score', update.get('score', 0.0))
                
                if 'old_score' in update and 'new_score' in update:
                    logger.info(f"Updated URL: {url} - Score: {update['old_score']} -> {update['new_score']}")
            
            self.conn.execute("COMMIT")
            
        except Exception as e:
            logger.error(f"Error updating records in DuckDB: {e}")
            self.conn.execute("ROLLBACK")
    
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.debug("DuckDB connection closed")
            except Exception as e:
                logger.error(f"Error closing DuckDB connection: {e}")