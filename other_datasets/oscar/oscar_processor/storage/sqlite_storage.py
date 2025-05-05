"""
SQLite storage implementation for OSCAR data processor.

This provides much faster updates than CSV for large datasets.
"""
import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Any, Optional

from oscar_processor.storage.base import BaseStorage
import os
logger = logging.getLogger(__name__)


class SQLiteStorage(BaseStorage):
    """SQLite-based storage for OSCAR data."""
    
    def __init__(self, output_path: str, filename: str = "oscar_data.db"):
        """
        Initialize SQLite storage.
        
        Args:
            output_path: Directory to save SQLite database
            filename: Name of the database file
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True, parents=True)
        self.db_file = self.output_path / filename
        self.conn = None
        self.cursor = None
        self.batch_size = 1000  # Number of records to commit at once
        self.csv_index = {}  # URL -> record mapping (for API compatibility)
        self.keep_csv_index = str(os.getenv("KEEP_CSV_INDEX", "False")).lower() == "true"
        logger.info(f"Keep CSV index: {self.keep_csv_index}")

    def initialize(self) -> None:
        """Initialize SQLite database and create tables if needed."""
        try:
            self.conn = sqlite3.connect(str(self.db_file))
            self.cursor = self.conn.cursor()
            
            # Enable WAL mode for better concurrent performance
            self.cursor.execute("PRAGMA journal_mode=WAL;")
            
            # Create table if it doesn't exist
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS oscar_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content TEXT NOT NULL,
                    warc_target_uri TEXT UNIQUE NOT NULL,
                    warc_date TEXT,
                    content_type TEXT
                )
            ''')
            
            # Create index on URL for fast lookups
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_warc_target_uri 
                ON oscar_data (warc_target_uri)
            ''')
            
            self.conn.commit()
            logger.info(f"Initialized SQLite database at {self.db_file}")
            
            if self.keep_csv_index:
                # Build index
                self.csv_index = self.build_index()
            
        except Exception as e:
            logger.error(f"Error initializing SQLite database: {e}")
            self.close()
            raise
    
    def build_index(self) -> Dict[str, Dict[str, Any]]:
        """
        Build an index of URLs from the database.
        
        Returns:
            Dictionary mapping URLs to record data
        """
        index = {}
        
        if not self.conn:
            logger.error("Database connection not initialized")
            return index
        
        logger.info("Building index from SQLite database")
        try:
            # Query all records but fetch in batches to manage memory
            self.cursor.execute("SELECT id, content, warc_target_uri, warc_date, content_type FROM oscar_data")
            
            batch_size = 10000
            batch = self.cursor.fetchmany(batch_size)
            
            while batch:
                for row_id, content, url, warc_date, content_type in batch:
                    index[url] = {
                        'row_idx': row_id,
                        'content': content,
                        'warc_date': warc_date,
                        'content_type': content_type
                    }
                
                batch = self.cursor.fetchmany(batch_size)
            
            logger.info(f"Loaded {len(index)} entries into index")
            
        except Exception as e:
            logger.error(f"Error building SQLite index: {e}")
        
        return index
    
    def get_record(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a record by URL.
        
        Args:
            url: URL to look up
            
        Returns:
            Record dictionary if found, None otherwise
        """
        # Fast lookup from memory index
        if url in self.csv_index:
            return self.csv_index[url]
        
        # Fallback to database query
        try:
            self.cursor.execute(
                "SELECT id, content, warc_date, content_type FROM oscar_data WHERE warc_target_uri = ?",
                (url,)
            )
            row = self.cursor.fetchone()
            
            if row:
                row_id, content, warc_date, content_type = row
                record = {
                    'row_idx': row_id,
                    'content': content,
                    'warc_date': warc_date,
                    'content_type': content_type
                }
                
                # Update memory index
                self.csv_index[url] = record
                return record
                
        except Exception as e:
            logger.error(f"Error retrieving record for URL {url}: {e}")
        
        return None
    
    def save_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Save a batch of new records to the database.
        
        Args:
            batch: List of dictionaries containing record data
        """
        if not batch:
            return
            
        try:
            # Use executemany for better performance
            self.cursor.executemany(
                '''
                INSERT OR IGNORE INTO oscar_data (content, warc_target_uri, warc_date, content_type)
                VALUES (?, ?, ?, ?)
                ''',
                [
                    (item['content'], item['warc_target_uri'], item['warc_date'], item['content_type'])
                    for item in batch
                ]
            )
            self.conn.commit()
            if self.keep_csv_index:
                # Update memory index for newly added items
                for item in batch:
                    url = item['warc_target_uri']
                    
                    # Check if it was actually inserted (might have been ignored due to UNIQUE constraint)
                    if url not in self.csv_index:
                        # Query the id of the newly inserted row
                        self.cursor.execute(
                            "SELECT id FROM oscar_data WHERE warc_target_uri = ?",
                            (url,)
                        )
                        row = self.cursor.fetchone()
                        
                        if row:
                            self.csv_index[url] = {
                                'row_idx': row[0],
                                'content': item['content'],
                                'warc_date': item['warc_date'],
                                'content_type': item['content_type']
                            }
                
            logger.debug(f"Saved {len(batch)} new records to SQLite")
            
        except Exception as e:
            logger.error(f"Error saving batch to SQLite: {e}")
            # Rollback in case of error
            self.conn.rollback()
    
    def update_records(self, updates: List[Dict[str, Any]]) -> None:
        """
        Update existing records in the database.
        
        Args:
            updates: List of dictionaries containing updated record data
        """
        if not updates:
            return
            
        logger.info(f"Updating {len(updates)} records with better scores")
        
        try:
            # Process updates in smaller batches
            for i in range(0, len(updates), self.batch_size):
                batch = updates[i:i + self.batch_size]
                
                for update in batch:
                    url = update['warc_target_uri']
                    
                    self.cursor.execute(
                        '''
                        UPDATE oscar_data
                        SET content = ?, warc_date = ?, content_type = ?
                        WHERE warc_target_uri = ?
                        ''',
                        (update['content'], update['warc_date'], update['content_type'], url)
                    )
                    if self.keep_csv_index:
                        # Update memory index
                        if url in self.csv_index:
                            self.csv_index[url]['content'] = update['content']
                            self.csv_index[url]['warc_date'] = update['warc_date']
                            self.csv_index[url]['content_type'] = update['content_type']
                    
                    if 'old_score' in update and 'new_score' in update:
                        logger.info(f"Updated URL: {url} - Score: {update['old_score']} -> {update['new_score']}")
                
                self.conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating records in SQLite: {e}")
            self.conn.rollback()
    
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            try:
                self.conn.commit()
                self.conn.close()
                self.conn = None
                self.cursor = None
                logger.debug("SQLite connection closed")
            except Exception as e:
                logger.error(f"Error closing SQLite connection: {e}")

    def export_to_csv(self, csv_path: str) -> None:
        """
        Export database contents to a CSV file.
        
        Args:
            csv_path: Path to CSV file
        """
        try:
            import pandas as pd
            
            logger.info(f"Exporting database to CSV: {csv_path}")
            
            # Query data in chunks to handle large databases
            chunk_size = 100000
            offset = 0
            first_chunk = True
            
            while True:
                query = f"""
                    SELECT content, warc_target_uri, warc_date, content_type
                    FROM oscar_data
                    ORDER BY id
                    LIMIT {chunk_size} OFFSET {offset}
                """
                
                df_chunk = pd.read_sql_query(query, self.conn)
                
                if df_chunk.empty:
                    break
                
                # Write header only for the first chunk
                df_chunk.to_csv(
                    csv_path, 
                    mode='w' if first_chunk else 'a',
                    header=first_chunk,
                    index=False
                )
                
                first_chunk = False
                offset += chunk_size
                logger.debug(f"Exported {offset} records to CSV")
            
            logger.info(f"Successfully exported database to {csv_path}")
            
        except Exception as e:
            logger.error(f"Error exporting database to CSV: {e}")