
'''
# server: crawled data
* manage crawled data
'''


from dotenv import load_dotenv
import os
from pathlib import Path
from s3_data_manager import DataProcessor
import threading


load_dotenv('.env')

def process_crawled_data():
    dp=DataProcessor()
    while True:
        # 1) download zip files
        downloaded_files = dp.download_zips()   # e.g. [PosixPath('downloads/a43579c1-17d0-4027-9661-0f30495b4c7b.zip')]

        # 2) extract zip files
        extracted_files = dp.extract_zips(downloaded_files)

        # 3) process pickle files to parquet
        parquet_file = dp.process_pickles_to_parquet()

        # 4) cleanup
        dp.cleanup(remove_downloads=True, remove_extracted=True)

        # sleep for 2 minutes
        time.sleep(120)

crawled_data_thread = threading.Thread(target=process_crawled_data, name='CrawledDataThread')
crawled_data_thread.daemon = True
crawled_data_thread.start()