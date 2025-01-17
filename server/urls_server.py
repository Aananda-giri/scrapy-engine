from bloom import get_bloom_thread

import csv
from datetime import datetime, timedelta
import json
import logging
from mongo import Mongo
import os
from pathlib import Path
import pickle
import time
from typing import List, Dict, Optional, Any

class URLManager:
    def __init__(self,
            collection, logger,
            error_bloom,
            to_crawl_bloom,
            local_storage_path: str = "local_storage",
            max_mongo_urls: int = 300_000, min_mongo_urls: int = 200_000,
            batch_size: int = 100_000,
            output_dir: str = "./output",
        ):
        
        if logger != None:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
        self.output_dir = Path("./output")
        self.redirect_links_dir = Path(self.output_dir / "redirect_links").resolve()  # Resolve to absolute path

        self.collection = collection
        self.local_storage_path = local_storage_path
        self.max_mongo_urls = max_mongo_urls
        self.min_mongo_urls = min_mongo_urls
        self.batch_size = batch_size
        
        self.error_bloom = error_bloom
        self.to_crawl_bloom = to_crawl_bloom
        # self.crawled_bloom = get_bloom_thread(save_file='crawled_bloom_filter.pkl', scalable=True)
        
        


        self.error_csv_path = Path('output/error_data/error_data.csv')
        
        # Create local storage directory if it doesn't exist
        os.makedirs(local_storage_path, exist_ok=True)
        os.makedirs(Path('output/error_data'), exist_ok=True)


    def add_url_to_mongo(self, url: str) -> bool:
        """Add a new URL to crawl if it hasn't been crawled or errored before."""
        if len(url) > 250:
            self.logger.info("len. url > 250. skipping..")
            return False
            
        if (url in self.crawled_bloom or 
            url in self.error_bloom or url in self.to_crawl_bloom):
            return False

        # Check if URL already exists in MongoDB
        if self.collection.find_one({"url": url}):
            return False

        doc = {
            "url": url,
            "status": "to_crawl",
            "timestamp": time.time(),
            "crawling_count": 0
        }
        self.collection.insert_one(doc)
        return True

    def get_url_to_crawl(self) -> Optional[str]:
        """Get a URL to crawl and update its status."""
        doc = self.collection.find_one_and_update(
            {"status": "to_crawl"},
            {
                "$set": {
                    "status": "crawling",
                    "timestamp": time.time()
                }
            },
            return_document=True
        )
        return doc["url"] if doc else None
    
    def get_urls_to_crawl(self, n: int) -> Optional[list[str]]:
        """Get n URLs to crawl and update their status."""
        docs = self.collection.find({"status": "to_crawl"}).limit(n)
        urls = []

        for doc in docs:
            updated = self.collection.find_one_and_update(
                {"_id": doc["_id"], "status": "to_crawl"},
                {
                    "$set": {
                        "status": "crawling",
                        "timestamp": time.time()
                    }
                },
                return_document=True
            )
            if updated:
                urls.append(updated["url"])

        return urls if urls else None
    # def mark_url_crawled(self, url: str):
    #     """Mark a URL as successfully crawled."""
    #     self.collection.delete_one({"url": url})
    #     self.crawled_bloom.add(url)
    #     self.crawled_bloom.save()

    def check_timeout_urls(self, timeout_minutes: int = 30) -> None:
        """
        Check for URLs that have timed out during crawling and handle them appropriately.
        
        Args:
            timeout_minutes: Minutes after which a crawling URL is considered timed out
        """
        timeout_threshold = time.time() - (timeout_minutes * 60)
        
        try:
            # Find timed out URLs - use projection to only get needed fields
            timed_out_urls = self.collection.find(
                {
                    "status": "crawling",
                    "timestamp": {"$lt": timeout_threshold}
                },
                {
                    "url": 1, 
                    "crawling_count": 1, 
                    "timestamp": 1
                }
            )
            
            error_data = []
            urls_to_delete = []
            
            # Process timed out URLs in batches
            for doc in timed_out_urls:
                if doc["crawling_count"] >= 3:
                    # update bloom filter
                    self.error_bloom.add(doc["url"])
                    urls_to_delete.append(doc["_id"])
                    
                    error_data.append({
                        'url': doc["url"],
                        'timestamp': time.time(),
                        'status': "error",
                        'status_code': 0,
                        'error_type': "max_retries_exceeded: crawling count > 3"
                    })
                else:
                    # Batch update for retry URLs
                    self.collection.update_one(
                        {"_id": doc["_id"]},
                        {
                            "$set": {
                                "status": "to_crawl",
                                "timestamp": time.time()
                            },
                            "$inc": {"crawling_count": 1}
                        }
                    )
            
            if error_data:
                self._append_error_links_to_csv(error_data)
                
            if urls_to_delete:
                self.collection.delete_many({"_id": {"$in": urls_to_delete}})
                self.logger.info(f"Removed {len(urls_to_delete)} failed URLs after max retries")
            
            # Save bloom filter (was updated previously)
            self.error_bloom.save()
                
        except Exception as e:
            self.logger.error(f"Error checking timeout URLs: {str(e)}")
            raise
    

    def manage_storage(self):
        """Manage MongoDB storage by moving URLs to/from local storage."""
        mongo_count = self.collection.count_documents({})
        mongo_count < self.min_mongo_urls

        if mongo_count > self.max_mongo_urls:
            self._save_to_local_storage()
        elif mongo_count < self.min_mongo_urls:
            self._load_from_local_storage()

    def _save_to_local_storage(self):
        """Save batch_size URLs to local storage."""
        urls_to_save = self.collection.find(
            {"status": "to_crawl"}
        ).limit(self.batch_size)

        if urls_to_save:
            timestamp = int(time.time())
            filename = f"{self.local_storage_path}/urls_{timestamp}.json"
            
            with open(filename, 'w') as f:
                json.dump(list(urls_to_save), f)
            
            # Remove saved URLs from MongoDB
            url_ids = [doc["_id"] for doc in urls_to_save]
            self.collection.delete_many({"_id": {"$in": url_ids}})
    def _load_from_local_storage(self) -> int:
            """
            Load URLs from oldest file in local storage to MongoDB.
            
            Returns:
                int: Number of URLs loaded to MongoDB
            """
            storage_path = Path(self.local_storage_path)
            storage_files = list(storage_path.glob('*.json'))
            
            if not storage_files:
                self.logger.info("No files found in local storage")
                return 0
                
            try:
                # Load from oldest file first
                oldest_file = min(storage_files)
                
                with oldest_file.open('r') as f:
                    urls = json.load(f)
                
                if urls:
                    result = self.collection.insert_many(urls, ordered=False)
                    inserted_count = len(result.inserted_ids)
                    self.logger.info(f"Loaded {inserted_count} URLs from {oldest_file}")
                
                # Remove the processed file
                oldest_file.unlink()
                return len(urls) if urls else 0
                
            except Exception as e:
                self.logger.error(f"Error loading from local storage: {str(e)}")
                raise
    
    
    def _append_error_links_to_csv(self, urls: List[Dict[str, Any]]) -> None:
        """
        Append error URLs to CSV file.
        
        Args:
            urls: List of dictionaries containing error URL data
        """
        fieldnames = ['url', 'timestamp', 'status', 'status_code', 'error_type']
        
        try:
            file_exists = self.error_csv_path.exists()
            
            with self.error_csv_path.open('a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    self.logger.info(f"Creating new CSV file: {self.error_csv_path}")
                    writer.writeheader()
                
                writer.writerows(urls)
                
        except Exception as e:
            self.logger.error(f"Error writing to CSV: {str(e)}")
            raise

    

    def save_error_data_from_mongo(self) -> None:
        """
        Process error URLs from MongoDB:
        0. get error links from mongo (saved by worker spider)
        1. Add them to error bloom filter (with url)
        2. Save them to CSV
        3. Remove them from MongoDB
        """
        try:
            # Get all error URLs in one query - use projection
            error_urls = self.collection.find(
                {'status': 'error'},
                {
                    'url': 1,
                    'timestamp': 1,
                    'status': 1,
                    'status_code': 1,
                    'error_type': 1
                }
            )
            
            error_data = []
            urls_to_delete = []
            
            for doc in error_urls:
                self.error_bloom.add(doc['url'])
                urls_to_delete.append(doc['_id'])
                
                error_data.append({
                    'url': doc['url'],
                    'timestamp': doc['timestamp'],
                    'status': doc['status'],
                    'status_code': doc.get('status_code'),  # Using get() for optional fields
                    'error_type': doc.get('error_type', 'unknown')
                })
            
            if error_data:
                self._append_error_links_to_csv(error_data)
                self.logger.info(f"Saved {len(error_data)} error URLs to CSV")
            
            if urls_to_delete:
                self.collection.delete_many({"_id": {"$in": urls_to_delete}})
                self.logger.info(f"Removed {len(urls_to_delete)} error URLs from MongoDB")
            
            # Save bloom filter after successful processing
            self.error_bloom.save()
            
        except Exception as e:
            self.logger.error(f"Error saving error data: {str(e)}")
            raise
    def push_error_files_to_hub(self):
        # 1) calculate file size in Mb
        error_file_size_mb = self.error_csv_path.stat().st_size / (1024 * 1024) if self.error_csv_path.exists() else 0

        if error_file_size_mb > 20:
            # 24*60*60 = 86400
            
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
            filename_at_hf = f"{uuid.uuid4()}{_timestamp}.csv"
            pushed_successfully = True
            try:
            
                # 2) push to hub
                self.hf_api.upload_file(
                    path_or_fileobj=self.error_csv_path,
                    path_in_repo=f'scrapy_engine/error_files/{filename_at_hf}',
                    repo_id="Aananda-giri/nepali_llm_datasets",
                    repo_type="dataset",
                    token=os.getenv('HF_TOKEN')
                )
                self.logger.info(f"pushed error data file to hub {self.error_csv_path} {parquet_file_size_mb}Mb ")
                
            except Exception as e:
                self.logger.error(f"Hugging Face upload failed: {str(e)}")

    def mongo_to_crawl_refill(self):
        '''
        * list of redirect urls (from crawled_data) are saved in multiple pickle_files @ `output/redirect_links`
        * this function loads those redirect_links to mongo (after making sure they've not already been crawled by checking bloom filters)

        * avoid loading `_temp.pickle` files

        The function:
            1. Reads redirect URLs from pickle files in the redirect_links directory
            2. Filters URLs using bloom filters to avoid duplicates
            3. Adds new URLs to MongoDB with metadata
            4. Updates and saves bloom filters
            5. Cleans up processed pickle files
            
        Returns:
            int: Number of new URLs added to MongoDB
        '''
        mongo_urls_count = self.collection.count_documents({})
        if mongo_urls_count > self.min_mongo_urls:
            # mongo have enough urls to crawl (no need for new to_crawl urls)
            # print('mongo already filled.')
            return 0

        # new links uploaded to mongo
        required_new_url_count = self.min_mongo_urls -  mongo_urls_count
        # print(f'required: {required_new_url_count}')
        
        # 1) Get all non-temporary pickle files
        pickle_files = [
            f for f in self.redirect_links_dir.glob("*.pickle")
            if not f.name.endswith("_temp.pickle")
        ]
        # print(f'pickle_files:{pickle_files}')

        pickle_files_loaded = []    # to delete later
        if not pickle_files:
            self.logger.info("mongo_to_crawl_refill: No pickle files found to process")
            return 0

        links_to_crawl = set()  # Using set for better performance

        # 2) Process each pickle file
        for pickle_path in pickle_files:
            try:    
                
                with open(pickle_path,'rb') as file:
                    redirect_links = pickle.load(file)
                    
                    # 2) Filter links through bloom filters (link should not be in any bloom filters)
                    new_links = {
                        link for link in redirect_links
                        if link not in self.error_bloom
                        and link not in self.to_crawl_bloom
                    }
                    # print(f'new_links: {new_links}')

                    # 2.5) Update bloom filters with new links
                    for link in new_links:
                        self.to_crawl_bloom.add(link)
                    
                    links_to_crawl.update(new_links)
                    # print(f'links_to_crawl: {links_to_crawl}')

                    # update loaded pickle files
                    pickle_files_loaded.append(pickle_path)
                    
                    # break early if sufficient links to_crawl links (tobe uploaded to mongo) are obtained
                    if len(links_to_crawl) >= required_new_url_count:
                        break
            
            except Exception as ex:
                self.logger.error(f"mongo_to_crawl_refill: Failed to process {pickle_path}: {str(ex)}")
                continue
                    
                
        # Prepare documents for MongoDB
        if links_to_crawl:
            current_time = time.time()
            docs = [
                {
                    "url": link,
                    "status": "to_crawl",
                    "timestamp": current_time,
                    "crawling_count": 0
                } for link in links_to_crawl
            ]
            
            # Batch insert into MongoDB
            try:
                result = self.collection.insert_many(docs, ordered=False)
                inserted_count = len(result.inserted_ids)
                self.logger.info(f"Added {inserted_count} new URLs to MongoDB")
            except Exception as e:
                self.logger.error(f"Failed to insert documents to MongoDB: {str(e)}")
                # Still save bloom filters even if MongoDB insert fails
                raise
            
            # Save bloom filters after successful MongoDB insertion
            try:
                self.to_crawl_bloom.save()  # it was updated
                self.logger.info("to_crawl Bloom filters updated and saved")
            except Exception as e:
                self.logger.error(f"Failed to save bloom filters: {str(e)}")
                raise
                
        # Clean up processed pickle files (only remove pickle_files_loaded
        for pickle_path in pickle_files_loaded:
            try:
                os.remove(pickle_path)
                # pickle_path.unlink()
            except Exception as e:
                self.logger.error(f"url_server(l.452):Failed to remove {pickle_path}: {str(e)}")
        
        return len(links_to_crawl)

def run_url_server(collection, logger, error_bloom, to_crawl_bloom):
    """Main crawler loop."""
    
    url_manager = URLManager(collection = collection, logger=logger, error_bloom=error_bloom, to_crawl_bloom=to_crawl_bloom)  # Using your existing MongoDB collection
    while True:
        try:
            # # load urls from .pickle files at `output/redirect_links``
            #  and save them to mongo after checking bloom filters: crawled_bloom_filter and error_bloom_filter
            url_manager.mongo_to_crawl_refill()
            
            # Check for timed out URLs
            url_manager.check_timeout_urls()

            # save_error_data_from_mongo
            url_manager.save_error_data_from_mongo()

            # push error file to hub if size>20Mb
            url_manager.push_error_files_to_hub()

            # (no longer needed: selective upload by `mongo_to_crawl_refill` should do) 
            # url_manager.manage_storage()
            
            # Save error links (added to mongo by worker)


            # Small sleep to prevent overwhelming MongoDB
            time.sleep(60)
        except Exception as e:
            print(f"Error in crawler loop: {e}")
            time.sleep(1)  # Sleep longer on error

if __name__ == "__main__":
    mongo = Mongo()
    logger = logging.getLogger(__name__)
    run_url_server(collection, logger)