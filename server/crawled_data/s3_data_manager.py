import os
import sys
import zipfile
import pickle
from pathlib import Path
import requests
import duckdb
import pandas as pd
from typing import List, Optional, Dict, Any
import logging
from urllib.parse import urlparse
from tqdm import tqdm

sys.path.append(str(Path('../scrapy_engine/scrapy_engine').resolve()))
from s3_v2 import Ec2Functions

class DataProcessor:
    def __init__(self, 
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
                               batch_size: int = 1000) -> Path:
        """
        Convert pickle files to a single Parquet file using DuckDB.
        
        Args:
            pickle_files: Optional list of pickle files to process.
                        If None, processes all pickle files in extract_dir.
            batch_size: Number of records to process at once
            
        Returns:
            Path to the updated or created Parquet file.
        """
        if pickle_files is None:
            pickle_files = list(self.extract_dir.glob('*.pickle'))
        
        # Determine output parquet file path
        parquet_path = self.output_dir / "combined_data.parquet"
        
        # Create a DuckDB table for temporary data processing
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS temp_data (
                url VARCHAR,
                html_content VARCHAR,
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
                        batch_data.append({
                            'url': data.get('url', ''),
                            'html_content': data.get('html_content', ''),
                            'timestamp': data.get('timestamp', pd.Timestamp.now())
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
        return parquet_path


    def cleanup(self, remove_downloads: bool = False, remove_extracted: bool = True):
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
                
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")

    def parquet_to_csv(self):
        '''
            Convert all Parquet files in the output directory to CSV using DuckDB.
            This is useful for verifying that Parquet files are being created correctly.
        '''
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

if __name__ == "__main__":
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