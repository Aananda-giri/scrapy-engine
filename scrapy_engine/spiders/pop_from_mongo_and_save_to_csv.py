import os
import time
import csv
import json
import redis
import shutil
import threading
from mongo import Mongo

from dotenv import load_dotenv
load_dotenv()


mongo = Mongo()


def display_stats():
    '''
        This is a thread to display the stats of the crawling process every 1 minute
    '''
    while True:
        # -----------------------------------------------------------------------
        # --------------------------------- Stats ------------------------------- 
        # -----------------------------------------------------------------------
        # there is no data
        # length of to_crawl
        to_crawl_count = mongo.collection.count_documents({"status": "to_crawl"})
        crawling_count = mongo.collection.count_documents({"status": "crawling"})
        crawled_count = mongo.collection.count_documents({"status": "crawled"})
        
        # Nice formatted view for to_crawl, crawled and crawling
        # Formatted output
        print("=================================================")
        print("\n #  *********** Crawl Queue Status ***********")
        print("=================================================")
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}')
        print(f"To Crawl: {locale.format_string('%d', to_crawl_count, grouping=True)}")
        print(f"Crawled: {locale.format_string('%d', crawled_count, grouping=True)}")
        print(f"Crawling: {locale.format_string('%d', crawling_count, grouping=True)}")
        
        # Get mongo stats
        db=mongo.db
        stats = db.command("dbstats")
        # Print the stats
        print("DB Size: ", stats['dataSize']/(1024*1024))
        print("Storage Size: ", stats['storageSize']/(1024*1024))
        print("Mongo Free Storage Space: ", stats['totalFreeStorageSize']/(1024*1024), end="\n-----------------------------------------------\n")
        
        # Crawling Rate
        newly_crawled = crawled_count - number_of_links_crawled_at_start
        time_taken = time.time() - start_time
        crawling_rate = newly_crawled / time_taken
        # print(f"Crawling Rate: {locale.format_string('%d', crawling_rate, grouping=True)} links/sec")
        expected_time_to_crawl = to_crawl_count / crawling_rate
        print(f"Crawling Rate: {crawling_rate} links/sec")
        print(f"Expected Time to Crawl: {locale.format_string('%d', expected_time_to_crawl/(60*60*24), grouping=True)} days")
        
        # Get Crawled File Size
        print(f"Size of crawled_data.csv: {os.path.getsize('crawled_data.csv')/(1024*1024) if os.path.exists('crawled_data.csv') else 0} MB", end="\n-----------------------------------------------\n")
        # -----------------------------------------------------------------------
        # Sleep for 1 minute
        time.sleep(60)


def backup_crawled_data():
    # ---------------------------------------------------------------------------
    ### Backup file named crawled_data.csv & other_data.csv after every 12 hours
    # ---------------------------------------------------------------------------
    while True:
        # Backup once for every 12 hours
        time.sleep(12 * 60 * 60)  # Sleep for 12 hours
        # Check if 'crawled_data.csv' exists
        if 'crawled_data.csv' in os.listdir():
            # get latest backup index
            backup_indices = [int(file.split('_')[-1].split('.')[0]) for file in os.listdir() if file.startswith('crawled_data_')]
            print(backup_indices)
            if backup_indices:
                # Get maximum index
                max_index = max(backup_indices)
            else:
                max_index = 0
        try:
            # backup 'crawled_data.csv' with index max_index+1
            shutil.copy('crawled_data.csv', f'crawled_data_backup_{max_index+1}.csv')
        except Exception as ex:
            print(ex)
            # log the error
            logging.exception(ex)
        try:
            # backup 'other_data.csv' with index max_index+1
            shutil.copy('other_data.csv', f'other_data_backup_{max_index+1}.csv')
        except Exception as ex:
            print(ex)
            # log the error
            logging.exception(ex)



def save_to_csv(data, data_type="crawled_data"):
    for key, data_items in data.items():
        csv_file_path = key + ".csv"
        if data_items:
            # field_names = ['paragraph', 'parent_url', 'page_title', 'is_nepali_confidence']
            field_names = data_items[0].keys()
            print(field_names)
            # break
            file_exists = os.path.exists(csv_file_path)
            # print(f'file_exists: {file_exists}')
            # Open the CSV file in append mode
            with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
                # Create a CSV writer object
                csv_writer = csv.DictWriter(csvfile, fieldnames=field_names)
                
                # If the file doesn't exist, write the header
                if not file_exists:
                    csv_writer.writeheader()
                
                # Append the new data
                csv_writer.writerows(data_items)




crawled_data = list(mongo.db['crawled_data'].find())
other_data = list(mongo.db['other_data'].find())

combined_data = {"crawled_data":crawled_data, "other_data":other_data}

# Save to .csv file
save_to_csv(combined_data)

# Delete multiple data by id
mongo.db['crawled_data'].delete_many({"_id": {"$in": [data['_id'] for data in crawled_data]} })
mongo.db['other_data'].delete_many({"_id": {"$in": [data_ot['_id'] for data_ot in other_data]} })

# if len(crawled_data)>0:
#     logging.info(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}: consumed: {len(crawled_data) + len(other_data)}')
#     print(f'consumed: {len(crawled_data) + len(other_data)}')     #\n\n current_count:{redis_client.llen("paragraphs")}')
