import os
import uuid
import zipfile
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from .s3_v2 import Ec2Functions
from huggingface_hub import HfApi
from dotenv import load_dotenv


load_dotenv('.env')
os.environ.get('uri')
assert os.environ.get('HF_TOKEN') != None, "scrapy_engine/scrapy_engine/background_upload.py: HF_TOKEN not found"


class BackgroundUploadService:
    def __init__(self, 
                 pickle_dir: str = "./",
                 size_threshold_mb: float = 100.0,
                 upload_interval_seconds: int = 3600,
                 check_interval_seconds: int = 300):
        """Initialize the background upload service with configurable parameters."""
        
        self.pickle_dir = Path(pickle_dir).resolve()  # Resolve to absolute path
        self.size_threshold_mb = size_threshold_mb
        self.upload_interval_seconds = upload_interval_seconds
        self.check_interval_seconds = check_interval_seconds
        
        self.hf_api = HfApi()
        self.setup_logging()

    def setup_logging(self):
        """Configure logging for the service."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('upload_service.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_pickle_files(self) -> List[Path]:
        """Get all non-temporary pickle files in the directory."""
        # self.logger.info(f"listdir:{os.listdir()}")
        return [
            f for f in self.pickle_dir.glob("*.pickle")
            if not f.name.endswith("_temp.pickle")
        ]

    def get_pickles_size(self) -> float:
        """Calculate total size of pickle files in MB."""
        pickle_files = self.get_pickle_files()
        return sum(f.stat().st_size / (1024 * 1024) for f in pickle_files)

    def zip_pickles(self) -> Optional[str]:
        """Zip pickle files and create a manifest."""
        try:
            pickle_files = self.get_pickle_files()
            if not pickle_files:
                self.logger.info("No pickle files to zip")
                all_pickles = [
                    f for f in self.pickle_dir.glob("*.pickle")
                ]
                # self.logger.info(f"self.pickle_dir:{self.pickle_dir} all_pickles:{all_pickles}")
                # self.logger.info(f"listdir:{os.listdir('pickles/')}")
                return None

            zip_filename = f"{uuid.uuid4()}.zip"
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                self.logger.info(f'Zipping {len(pickle_files)} pickle files')
                
                # Add files to zip
                for file in pickle_files:
                    zipf.write(file, file.name)
                
                # Add manifest
                manifest = f"""Backup created on: {datetime.now()}
                            Total files: {len(pickle_files)}
                            Files included:
                            {chr(10).join(f'- {f.name}' for f in pickle_files)}"""
                zipf.writestr("manifest.txt", manifest)
            self.logger.info(f'pickles compressed to {zip_filename} ')

            # Delete original files only after successful zip creation
            for file in pickle_files:
                file.unlink()
                self.logger.debug(f'Deleted original file: {file}')

            return zip_filename

        except Exception as e:
            self.logger.error(f"Error during zip operation: {str(e)}")
            return None

    def upload_to_huggingface(self, zip_filename: str) -> bool:
        """Upload zip file to Hugging Face."""
        try:
            self.logger.info(f'Uploading {zip_filename} to Hugging Face')
            self.hf_api.upload_file(
                path_or_fileobj=zip_filename,
                path_in_repo=f'scrapy_engine/raw_chunks/{zip_filename}',
                repo_id="Aananda-giri/nepali_llm_datasets",
                repo_type="dataset",
                token=os.getenv('HF_TOKEN')
            )
            return True

        except Exception as e:
            self.logger.error(f"Hugging Face upload failed: {str(e)}")
            return False

    def upload_to_s3(self, zip_filename: str) -> bool:
        """Upload zip file to S3 as fallback."""
        try:
            self.logger.info(f'Uploading {zip_filename} to S3')
            Ec2Functions.upload_file(
                file_path=zip_filename,
                bucket_name='1b-bucket',
                object_key=zip_filename
            )
            return True

        except Exception as e:
            self.logger.error(f"S3 upload failed: {str(e)}")
            return False

    def cleanup_zip(self, zip_filename: str):
        """Safely remove zip file after upload."""
        try:
            Path(zip_filename).unlink()
            self.logger.info(f'Cleaned up zip file: {zip_filename}')
        except Exception as e:
            self.logger.error(f"Failed to cleanup zip file: {str(e)}")

    def run(self):
        """Main service loop."""
        self.logger.info("Starting Background Upload Service")
        start_time = time.time()

        while True:
            try:
                time_elapsed = time.time() - start_time
                pickle_files_size_mb = self.get_pickles_size()
                
                self.logger.debug(f"Current pickle files size: {pickle_files_size_mb:.2f}MB")
                
                if time_elapsed > self.upload_interval_seconds or pickle_files_size_mb > self.size_threshold_mb:
                    self.logger.info("Initiating upload cycle")
                    start_time = time.time()  # Reset timer
                    
                    zip_filename = self.zip_pickles()
                    if zip_filename:
                        upload_success = False
                        
                        # Try s3 first
                        
                        # Fallback to HF if s3 fails
                        if self.upload_to_s3(zip_filename):
                            upload_success = True
                            self.logger.info("Upload to s3 successful.")
                        elif self.upload_to_huggingface(zip_filename):
                            upload_success = True
                            self.logger.info("Upload to HF successful.")
                            
                        if upload_success:
                            self.cleanup_zip(zip_filename)
                        else:
                            self.logger.error("Both upload attempts failed")
                
                time.sleep(self.check_interval_seconds)

            except Exception as e:
                self.logger.error(f"Error in main service loop: {str(e)}")
                time.sleep(self.check_interval_seconds)  # Continue despite errors

if __name__ == "__main__":
    service = BackgroundUploadService()
    service.run()