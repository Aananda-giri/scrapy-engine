"""
Main processor class for OSCAR data.
"""
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Literal


from .utils.nepali_text_score import NepaliTextScorer
from .utils.bloom import get_bloom_thread
from .utils.hf_functions import HFFunctions
from .storage import get_storage

logger = logging.getLogger(__name__)


class OscarDataProcessor:
    """
    Process OSCAR community data, filtering duplicates and optionally updating records.
    """
    
    def __init__(
        self, 
        output_path: str = "processed_data", 
        bloom_save_file: str = "oscar_community_bloom_filter.pkl",
        storage_type: Literal["csv", "sqlite", "parquet", "duckdb"] = "duckdb",
        storage_filename: Optional[str] = None
    ):
        """
        Initialize the OSCAR data processor.
        
        Args:
            output_path: Directory to save processed files
            bloom_save_file: Path to save/load Bloom filter
            storage_type: Type of storage backend to use ("csv", "sqlite", "parquet", or "duckdb")
            storage_filename: Custom filename for storage (uses default if None)
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True, parents=True)
        
        
        self.bloom_save_file = bloom_save_file
        self.bloom_filter = get_bloom_thread(save_file=bloom_save_file)
        self.text_scorer = NepaliTextScorer()

        self.hf_functions = HFFunctions()
        
        # Initialize storage backend
        self.storage = get_storage(
            storage_type=storage_type,
            output_path=output_path,
            filename=storage_filename
        )
        
        # Statistics
        self.urls_processed = 0
        self.urls_saved = 0
        self.urls_updated = 0

        self.batch_size = 15000

        self.ignore_duplicate = str(os.getenv("IGNORE_DUPLICATE", "False")).lower() == "true"
        logger.info(f"Ignore duplicate: {self.ignore_duplicate}")

    def run(self) -> None:
        """
        Main method to run the entire pipeline using the yield_rows function.
        """
        try:
            logger.info("Starting OSCAR data processing pipeline")
            start_time = time.time()
            
            # Process records in batches for better performance
            batch = []
            updates = []
            
            # Safety counter to prevent infinite loops
            row_count = 0
            
            # Use the yield_rows function from HFFunctions
            for row in self.hf_functions.yield_rows():
                if row == "end" or row == "the_end":
                    # save bloom filter
                    self.bloom_filter.save()
                    
                    # save in chunks
                    for i in range(0, len(batch), self.batch_size):
                        self.storage.save_batch(batch[i:i + self.batch_size])
                    batch = []
                    elapsed = time.time() - start_time
                    logger.info(f"----------------------------------------------------------------------------------------")
                    logger.info(f"Processed (saved) {self.urls_processed} records in {elapsed:.2f}s ({self.urls_processed/elapsed:.2f} records/s)")
                    logger.info(f"URLs1: (saved) {self.urls_processed} processed, {self.urls_saved} saved, {self.urls_updated} updated")
                    logger.info(f"----------------------------------------------------------------------------------------")
                    continue
                row_count += 1
                self.urls_processed += 1
                
                try:
                    # Extract required fields from the record
                    url = row.warc_headers.warc_target_uri
                    content = row.content
                    warc_date = row.warc_headers.warc_date
                    content_type = row.warc_headers.content_type
                    
                    if not url:
                        continue
                    
                    # Check if URL is in bloom filter
                    if url not in self.bloom_filter:
                        # Calculate score for the content
                        score = self.text_scorer.score_text(content)['overall_score']
                        
                        # URL not seen before, save it
                        batch.append({
                            'content': content,
                            'warc_target_uri': url,
                            'warc_date': warc_date,
                            'content_type': content_type,
                            'score': score  # Store the score in the record
                        })
                        
                        # Add URL to bloom filter
                        self.bloom_filter.add(url)
                        
                        self.urls_saved += 1
                    
                    else:
                        if self.ignore_duplicate:
                            # ignore duplicate entries
                            continue
                        
                        # URL already seen, check if we should update it
                        existing_record = self.storage.get_record(url)
                        if existing_record:
                            saved_content = existing_record['content']
                            
                            # Calculate scores for both pages
                            try:
                                saved_score = existing_record.get('score')
                                if saved_score is None:
                                    # Calculate score if not already stored
                                    saved_score = self.text_scorer.score_text(saved_content)['overall_score']
                                
                                new_score = self.text_scorer.score_text(content)['overall_score']
                                
                                # If new page has better score, update it
                                if new_score > saved_score:
                                    updates.append({
                                        'content': content,
                                        'warc_target_uri': url,
                                        'warc_date': warc_date,
                                        'content_type': content_type,
                                        'old_score': saved_score,
                                        'new_score': new_score
                                    })
                                    
                                    self.urls_updated += 1
                            except Exception as score_err:
                                logger.error(f"Error calculating text scores: {score_err}")
                    
                    # # Process batches if they reach the batch size
                    # if len(batch) >= self.batch_size:
                    #     self.storage.save_batch(batch)
                    #     batch = []
                    
                    if len(updates) >= self.batch_size:
                        self.storage.update_records(updates)
                        updates = []
                    
                    # Log progress periodically
                    if self.urls_processed > 0 and self.urls_processed % 50000 == 0:
                        # self.bloom_filter.save()
                        elapsed = time.time() - start_time
                        logger.info(f"Processed (not saved) {self.urls_processed} records in {elapsed:.2f}s ({self.urls_processed/elapsed:.2f} records/s)")
                        logger.info(f"URLs2: (not saved) {self.urls_processed} processed2, {self.urls_saved} saved, {self.urls_updated} updated")
                
                except Exception as row_error:
                    logger.error(f"Error processing record {row_count}: {row_error}")
                    continue
            
            # Save any remaining items in the batches
            if batch:
                self.storage.save_batch(batch)
            
            if updates:
                self.storage.update_records(updates)
            
            # Save bloom filter
            logger.info(f"Saving bloom filter to {self.bloom_save_file}")
            self.bloom_filter.save()
            self.bloom_filter.stop()
            
            # Close storage
            self.storage.close()
            
            elapsed = time.time() - start_time
            logger.info(f"OSCAR data processing pipeline completed successfully in {elapsed:.2f}s")
            logger.info(f"Total URLs: {self.urls_processed} processed, {self.urls_saved} saved, {self.urls_updated} updated")
        
        except Exception as e:
            logger.error(f"Error running pipeline: {e}")
            # Ensure resources are cleaned up even if error occurs
            self.bloom_filter.save()
            self.bloom_filter.stop()
            self.storage.close()