
'''
# server: crawled data
* manage crawled data
'''


from dotenv import load_dotenv
from datetime import datetime
import duckdb
from huggingface_hub import HfApi
import logging
import os
import sys

import pickle
from pathlib import Path

import pandas as pd
import requests
import time
from tqdm import tqdm
from typing import List, Optional, Dict, Any, Tuple

from urllib.parse import urlparse


sys.path.append(str(Path('../scrapy_engine/scrapy_engine').resolve()))
from s3_v2 import Ec2Functions
import zipfile

from mongo import Mongo

class DataProcessor:
    def __init__(self,
                collection, 
                logger, 
                download_dir: str = "./downloads",
                extract_dir: str = "./extracted",
                output_dir: str = "./output",
                logger: Optional[logging.Logger] = None
        ):
        """Initialize the data processor with configurable directories."""
        self.download_dir = Path(download_dir)
        self.extract_dir = Path(extract_dir)
        self.output_dir = Path(output_dir)
        if logger != None:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
        
        # Create necessary directories
        self.download_dir.mkdir(exist_ok=True)
        self.extract_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize DuckDB connection
        self.db = duckdb.connect(database=':memory:', read_only=False)

        # Initialize the huggingface api
        self.hf_api = HfApi()

        # self.mongo = Mongo()
        self.collection = collection

        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(extract_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

    def download_zips(self) -> List[Path]:
        """
        Download zip files from s3 bucket: '1b-bucket'.
        
        Returns:
            List of paths to downloaded files
        """
        ec2_functions = Ec2Functions()
        files_list = ec2_functions.list_files()
        # ec2_functions.upload_file('a43579c1-17d0-4027-9661-0f30495b4c7b.zip')

        for file_name in files_list:
            downloaded_zip_files = []
            if file_name.endswith('zip'):
                # ec2_functions.download_file('a43579c1-17d0-4027-9661-0f30495b4c7b.zip', folder_path='downloads')
                download_path = ec2_functions.download_file(file_name, folder_path=self.download_dir)
                if download_path:
                    downloaded_zip_files.append(download_path)
                    self.logger.info(f"Successfully downloaded: {file_name}")
                else:
                    self.logger.info(f'failed to download {file_name}')
        return downloaded_zip_files

    def extract_zips(self, zip_files: Optional[List[Path]] = None) -> List[Path]:
        """
        Extract zip files to the extraction directory.
        
        Args:
            zip_files: Optional list of specific zip files to extract.
                      If None, processes all zip files in download_dir.
        
        Returns:
            List of paths to extracted pickle files
        """
        if zip_files is None:
            zip_files = list(self.download_dir.glob('*.zip'))
        
        extracted_files = []
        
        for zip_path in tqdm(zip_files, desc="Extracting zip files"):
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # Only extract .pickle files
                    pickle_files = [f for f in zip_ref.namelist() if f.endswith('.pickle')]
                    
                    for pickle_file in pickle_files:
                        zip_ref.extract(pickle_file, self.extract_dir)
                        extracted_files.append(self.extract_dir / pickle_file)
                self.logger.info(f"Successfully extracted: {zip_path}")
                
            except Exception as e:
                self.logger.error(f"Failed to extract {zip_path}: {str(e)}")
        
        return extracted_files

    def process_pickles_to_parquet(self, 
                             pickle_files: Optional[List[Path]] = None,
                             batch_size: int = 1000) -> Tuple[Path, Path]:
        """
        Convert pickle files to a single Parquet file using DuckDB and save redirect links separately (to: self.output_dir / "redirect_links").
        
        Args:
            pickle_files: Optional list of pickle files to process.
                        If None, processes all pickle files in extract_dir.
            batch_size: Number of records to process at once
            
        Returns:
            Tuple containing:
            - Path to the updated or created Parquet file
            - Path to the directory containing redirect links pickle files
        """
        if pickle_files is None:
            pickle_files = list(self.extract_dir.glob('*.pickle'))
        
        # Determine output paths
        parquet_path = self.output_dir / "combined_data.parquet"
        redirect_links_dir = self.output_dir / "redirect_links"
        
        crawled_links_dir = self.output_dir / "crawled_links"
        redirect_links_dir.mkdir(exist_ok=True)
        crawled_links_dir.mkdir(exist_ok=True)
        
        # Create a DuckDB table for temporary data processing
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS temp_data (
                request_url VARCHAR,
                response_url VARCHAR,
                response_body VARCHAR,
                timestamp TIMESTAMP
            )
        """)
        
        # If the Parquet file exists, load its content into the table
        if parquet_path.exists():
            self.db.execute(f"""
                INSERT INTO temp_data
                SELECT * FROM read_parquet('{parquet_path}')
            """)
        
        # Process files in batches
        total_processed = 0
        for i in tqdm(range(0, len(pickle_files), batch_size), desc="Processing pickle files"):
            batch_files = pickle_files[i:i + batch_size]
            batch_data = []
            
            for pickle_path in batch_files:
                try:
                    with pickle_path.open('rb') as f:
                        data = pickle.load(f)
                        
                        # delete crawled urls from mongo
                        if 'request_url' in data:
                            temp_redirect_links_path = redirect_links_dir / f"redirect_links_{pickle_path.stem}_temp.pickle"
                            redirect_links_path = redirect_links_dir / f"redirect_links_{pickle_path.stem}.pickle"
                            
                            # Delete crawled urls from mongo
                            crawled_urls = list(set([data['response_url'], data['request_url']]))
                            if crawled_urls:
                                self.collection.delete_many({"url": {"$in": crawled_urls}})
                        
                        # Save crawled links to separate pickle file
                        if 'redirect_links' in data:
                            temp_redirect_links_path = redirect_links_dir / f"redirect_links_{pickle_path.stem}_temp.pickle"
                            redirect_links_path = redirect_links_dir / f"redirect_links_{pickle_path.stem}.pickle"
                            
                            with temp_redirect_links_path.open('wb') as rf:
                                pickle.dump(data['redirect_links'], rf)
                            
                            os.rename(temp_redirect_links_path, redirect_links_path)
                        
                        # Prepare main data for Parquet
                        batch_data.append({
                            'request_url': data.get('request_url', ''),
                            'response_url': data.get('response_url', ''),
                            'response_body': data.get('response_body', ''),
                            'timestamp': data.get('timestamp', pd.Timestamp.now())  # Using redirect_links as timestamp
                        })
                except Exception as e:
                    self.logger.error(f"Failed to process {pickle_path}: {str(e)}")
            
            if batch_data:
                # Convert batch to DataFrame and insert into DuckDB
                df = pd.DataFrame(batch_data)
                self.db.execute("INSERT INTO temp_data SELECT * FROM df")
                total_processed += len(batch_data)
        
        # Write updated data to the Parquet file
        self.db.execute(f"""
            COPY (SELECT * FROM temp_data) 
            TO '{parquet_path}' 
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """)
        
        self.logger.info(f"Successfully processed {total_processed} records to {parquet_path}")
        self.logger.info(f"Redirect links saved to {redirect_links_dir}")
        
        return parquet_path, redirect_links_dir
    def push_to_hub(self, parquet_file: Path, start_time: Optional[float] = 0.0):
        '''
        * condition:
          - once every 24 hours
          - once parquet file size is greater than 20Mb
        '''
        # 1) calculate file size in Mb
        parquet_file_size_mb = parquet_file.stat().st_size / (1024 * 1024)

        if parquet_file_size_mb > 20 or (time.time() - start_time) >= 86400:
            # 24*60*60 = 86400
            
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
            filename_at_hf = f"{uuid.uuid4()}{_timestamp}.pickle"
            pushed_successfully = True
            try:
            
                # 2) push to hub
                self.hf_api.upload_file(
                    path_or_fileobj=parquet_file,
                    path_in_repo=f'scrapy_engine/raw_parquets/{filename_at_hf}',
                    repo_id="Aananda-giri/nepali_llm_datasets",
                    repo_type="dataset",
                    token=os.getenv('HF_TOKEN')
                )
                
                self.logger.info(f"pushed pickle file to hub {parquet_file} {parquet_file_size_mb}Mb time_spend:{time.time() - start_time}s")
                
                # Reset start_time
                start_time = time.time()
                pushed_successfully = True
                return start_time, pushed_successfully
            except Exception as e:
                self.logger.error(f"Hugging Face upload failed: {str(e)}")
        return start_time, pushed_successfully
        
        # return sum(f.stat().st_size / (1024 * 1024) for f in pickle_files)
    def cleanup(self, remove_downloads: bool = False, remove_extracted: bool = True, remove_parquet:bool = True):
        """Clean up temporary files."""
        try:
            if remove_downloads:
                for file in self.download_dir.glob('*.zip'):
                    file.unlink()
                self.logger.info("Removed downloaded zip files")
            
            if remove_extracted:
                for file in self.extract_dir.glob('*.pickle'):
                    file.unlink()
                self.logger.info("Removed extracted pickle files")
            if remove_parquet:
                for file in self.output_dir.glob('*.parquet'):
                    file.unlink()
                self.logger.info("Removed parquet files")
                
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")

    def pickle_to_txt(self, folder_path: Optional[Path] = None) -> None:
        """
        Converts all pickle files in the specified folder to text files.
        
        Args:
            folder_path (Optional[Path]): Path to the folder containing pickle files.
        
        Raises:
            ValueError: If the provided folder path is None.
            FileNotFoundError: If the folder does not exist or is not a directory.
        """
        print('pickle_to_txt')
        if folder_path is None:
            raise ValueError("Folder path must be provided.")
        
        if not folder_path.exists() or not folder_path.is_dir():
            raise FileNotFoundError(f"The specified folder does not exist or is not a directory: {folder_path}")
        
        # Iterate through all pickle files in the folder
        for pickle_file in folder_path.glob("*.pickle"):
            try:
                print(pickle_file)
                # Load data from the pickle file
                with pickle_file.open('rb') as pf:
                    data = pickle.load(pf)
                
                # Determine the output text file path
                txt_path = pickle_file.with_suffix('.txt')
                
                # Write data to the text file
                with txt_path.open('w', encoding='utf-8') as txt_file:
                    if isinstance(data, (list, dict)):
                        for item in data:
                            txt_file.write(f"{item}\n")
                    else:
                        txt_file.write(str(data))
                
                print(f"Processed: {pickle_file.name} -> {txt_path.name}")
            
            except pickle.UnpicklingError:
                print(f"Error: The file {pickle_file.name} is not a valid pickle file or is corrupted.")
            except Exception as e:
                print(f"An unexpected error occurred with file {pickle_file.name}: {e}")


    def parquet_to_csv(self, path:Optional[Path]=None):
        '''
            Convert all Parquet files in the output directory to CSV using DuckDB.
            This is useful for verifying that Parquet files are being created correctly.
        '''
        if path:
            # Get all Parquet files in the output directory
            parquet_files = [Path(os.path.join(path, file)) for file in os.listdir(str(path)) if file.endswith('.pickle')]
        else:
            # Get all Parquet files in the output directory
            parquet_files = list(self.output_dir.glob('*.parquet'))
        
        if not parquet_files:
            self.logger.warning("No Parquet files found in the output directory.")
            return
        
        for parquet_file in parquet_files:
            try:
                # Define the output CSV file path
                csv_file = parquet_file.with_suffix('.csv')
                
                # Use DuckDB to convert the Parquet file to CSV
                self.db.execute(f"""
                    COPY (SELECT * FROM read_parquet('{parquet_file}'))
                    TO '{csv_file}' 
                    (FORMAT CSV, HEADER TRUE)
                """)
                
                self.logger.info(f"Converted {parquet_file} to {csv_file}")
            except Exception as e:
                self.logger.error(f"Failed to convert {parquet_file} to CSV: {str(e)}")


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper cleanup when used in with statement."""
        self.db.close()
        self.cleanup()

def run_crawled_data_server(collection, logger):
    """Main crawler loop."""
    dp=DataProcessor(collection=collection, logger=logger)
    start_time = time.time()

    while True:
        # 1) download zip files
        # downloaded_files = dp.download_zips()   # e.g. [PosixPath('downloads/a43579c1-17d0-4027-9661-0f30495b4c7b.zip')]
        
        # uncomment + rename zip file to debug
        downloaded_files =[ Path('downloads/d923cd47-c425-4eef-8cc8-e7bbe80bc432.zip')]
        
        # 2) extract zip files
        extracted_files = dp.extract_zips(downloaded_files)

        # 3) process pickle files to parquet
        parquet_file = dp.process_pickles_to_parquet()
        
        # # 4) push to hub once every 24 hours or once parquet file size is greater than 20Mb
        start_time, successfully_pushed_to_hub = dp.push_to_hub(parquet_file, start_time)

        # # # uncomment for testing
        # 5) convert parquet to csv
        # dp.parquet_to_csv()
        # dp.pickle_to_txt(Path('./output/redirect_links'))

        # # # 6) cleanup
        dp.cleanup(remove_downloads=True, remove_extracted=True, remove_parquet = successfully_pushed_to_hub)

        # sleep for 2 minutes
        time.sleep(120)
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    dp=DataProcessor()
    
    # 1) download zip files
    downloaded_files = dp.download_zips()   # e.g. [PosixPath('downloads/a43579c1-17d0-4027-9661-0f30495b4c7b.zip')]
    
    # 2) extract zip files
    extracted_files = dp.extract_zips(downloaded_files)

    # 3) process pickle files to parquet
    parquet_file = dp.process_pickles_to_parquet()

    # 4) cleanup
    dp.cleanup(remove_downloads=True, remove_extracted=True)

    # to make sure data is correctly stored in parquet file
    # 5) convert parquet to csv
    # dp.parquet_to_csv()