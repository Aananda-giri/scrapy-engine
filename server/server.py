import os
import sys
import time
import csv
import json
import redis
import shutil
import threading


from sqlite_handler import URLDatabase

from mongo import Mongo
mongo = Mongo()

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

from dotenv import load_dotenv
load_dotenv()


import logging
logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w", format="%(asctime)s - %(levelname)s - %(message)s")


'''
logging.debug("debug")       # logs all the logs below
logging.info("info")         # log all except debug log
logging.warning("warning")  # log warning, error, critical      (Default)
logging.error("error")      # log error, critical
logging.critical("critical")    # log critical

try:
    1/0
except Exception as ex:
    logging.exception(ex)
'''

# ======================================================
# Crawling  ==> to_crawl (if timestamp >2 hours)
# ======================================================


start_time = time.time()
number_of_links_crawled_at_start = mongo.collection.count_documents({"status": "crawled"})

def run_periodically():
  while True:
    # Convert urls from crawling to to_crawl if they are in crawling state for more than 2 hours
    mongo.recover_expired_crawling(created_before=7200)
    
    
    # Sleep for 3 hour (converted to seconds)
    time.sleep(3 * 60 * 60)

# Create and start the thread as a daemon
recover_expired_crawling_thread = threading.Thread(target=run_periodically)
recover_expired_crawling_thread.daemon = True
recover_expired_crawling_thread.start()
# ======================================================

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
        to_crawl_spider_count = mongo.collection.count_documents({"status": 'to_crawl?'})
        crawling_count = mongo.collection.count_documents({"status": "crawling"})
        crawled_count = mongo.collection.count_documents({"status": "crawled"})
        to_crawl_sqlite_count = URLDatabase(db_path="urls.db").count_entries("to_crawl")
        crawled_sqlite_count = URLDatabase(db_path="urls.db").count_entries("crawled")

        # Nice formatted view for to_crawl, crawled and crawling
        # Formatted output
        print("=================================================")
        print("\n #  *********** Crawl Queue Status ***********")
        print("=================================================")
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}')
        print(f"\"to_crawl\" (Mongo): {locale.format_string('%d', to_crawl_count, grouping=True)}")
        print(f"\"to_crawl?\" (Mongo by Spider): {locale.format_string('%d', to_crawl_spider_count, grouping=True)}")
        print(f"Crawling (Mongo): {locale.format_string('%d', crawling_count, grouping=True)}")
        print(f"\"to_crawl\" (Sqlite): {locale.format_string('%d', to_crawl_sqlite_count, grouping=True)}")
        print(f"\"crawled\" (Sqlite): {locale.format_string('%d', crawled_sqlite_count, grouping=True)}")
        # print(f"Crawled: {locale.format_string('%d', crawled_count, grouping=True)}")
        

        
        # Get mongo stats
        db=mongo.db
        stats = db.command("dbstats")
        # Print the stats
        # print("DB Size: ", stats['dataSize']/(1024*1024))
        # print("Storage Size: ", stats['storageSize']/(1024*1024))
        # print("Mongo Free Storage Space: ", stats['totalFreeStorageSize']/(1024*1024), end="\n-----------------------------------------------\n")
        
        # Crawling Rate
        newly_crawled = crawled_count - number_of_links_crawled_at_start
        time_taken = time.time() - start_time
        crawling_rate = newly_crawled / time_taken
        # print(f"Crawling Rate: {locale.format_string('%d', crawling_rate, grouping=True)} links/sec")
        expected_time_to_crawl = to_crawl_count / (crawling_rate if crawling_rate > 0 else 0.0000001)
        print(f"Crawling Rate: {crawling_rate} links/sec")
        print(f"Expected Time to Crawl: {locale.format_string('%d', expected_time_to_crawl/(60*60*24), grouping=True)} days")
        
        # Get Crawled File Size
        print(f"Size \"crawled_data.csv\": {os.path.getsize('crawled_data.csv')/(1024*1024) if os.path.exists('crawled_data.csv') else 0} MB")
        print(f"Size of \"other_data.csv\": {os.path.getsize('other_data.csv')/(1024*1024) if os.path.exists('other_data.csv') else 0} MB")
        print(f"Size of urls.db: {os.path.getsize('urls.db')/(1024*1024) if os.path.exists('urls.db') else 0} MB")
        print("===============================================\n\n\n")

        # -----------------------------------------------------------------------

        # Sleep for 3 minute
        time.sleep(60*3)


# =========================================================
# sqlite to save crawled and to_crawl urls (mongo is full)
# =========================================================

def to_crawl_cleanup_and_mongo_to_crawl_refill():
    '''
        * creating a giant thread to avoid concurrency issues
        * This thread will run once every 1.5 hours
    '''
    print("mongo_to_crawl_refill started")
    logging.info("mongo_to_crawl_refill started")
    url_db = URLDatabase(db_path="urls.db")
    while True:    
        # Get all urls from "to_crawl?"
        urls = mongo.collection.find({"status": 'to_crawl?'})
        to_crawl_urls_from_mongo = [(url['url'], url['timestamp']) for url in list(urls)]
        # to_crawl_urls
        # Save to sqlite
        # Insert the data into the database
        if to_crawl_urls_from_mongo:
            url_db.bulk_insert("crawled", to_crawl_urls_from_mongo, show_progress=False)
            # delete from mongo
            urls_to_delete = [url[0] for url in to_crawl_urls_from_mongo]
            mongo.collection.delete_many({'url': {'$in': urls_to_delete}, 'status': 'to_crawl?'})
        # -----------------------
        # mongo_to_crawl_refill
        # -----------------------
        
        if mongo.collection.count_documents({"status": 'to_crawl'}) < 100000:
            new_to_crawl_urls = url_db.fetch('to_crawl', 10000)
            n_failed_to_upload = 0
            try:
                # insert many
                mongo.collection.insert_many([{'url': url[0], 'status': 'to_crawl', 'timestamp': url[1]} for url in new_to_crawl_urls], ordered=False)
            except Exception as bwe:
                # pass
                # Get the details of the operations that failed
                failed_ops = bwe.details['writeErrors']
                
                # Get the documents that failed to insert
                failed_docs = [op['op'] for op in failed_ops]
                # Get the URLs that failed to insert
                urls_failed_to_upload_to_mongo = [(doc['url'], doc['timestamp']) for doc in failed_docs]
                n_failed_to_upload = len(urls_failed_to_upload_to_mongo)
                # for url in urls_failed_to_upload_to_mongo:
                #     logging.error(f"Failed to upload {url} to MongoDB")
                # success_urls = [url for url in new_to_crawl_urls if url not in urls_failed_to_upload_to_mongo]
                # # Delete successful urls from sqlite
                # url_db.delete("to_crawl", success_urls)
                # # print(f'success_urls:{success_urls}, len:{len(success_urls)}')
                # print(failed_urls, len(failed_urls))
            # # delete from sqlite
            # if n_failed_to_upload < 9900:
            url_db.delete("to_crawl", new_to_crawl_urls)
            # print(n_failed_to_upload)
            # else:
            #     print(f'failed to upload {n_failed_to_upload} urls to mongo')
            #     # exit the python script
            #     # sys.exit(1)
        print(f"inserted {10000-n_failed_to_upload} urls to mongodb")
        logging.info("inserted 100,000 urls to mongodb")
        # Sleep for 5 minutes
        time.sleep(5 * 60)


# ======================================================
# Save data from redis to csv file
# ======================================================

def pop_from_mongo():
    crawled_data = list(mongo.db['crawled_data'].find())
    other_data = list(mongo.db['other_data'].find())
    # print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}: ', end='')
    if not crawled_data and not other_data:
        # display_stats()
        # print('========sleeping for 5 sec.....==========')   # No Data
        time.sleep(10)
    # if time.time()%60 == 0:
    #     # display once every minute
    #     display_stats()
    combined_data = {"crawled_data":crawled_data, "other_data":other_data}
    
    # Save to .csv file
    save_to_csv(combined_data)
    
    # Delete multiple data by id
    mongo.db['crawled_data'].delete_many({"_id": {"$in": [data['_id'] for data in crawled_data]} })
    mongo.db['other_data'].delete_many({"_id": {"$in": [data_ot['_id'] for data_ot in other_data]} })
    
    # if len(crawled_data)>0:
    #     logging.info(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}: consumed: {len(crawled_data) + len(other_data)}')
    #     print(f'consumed: {len(crawled_data) + len(other_data)}')     #\n\n current_count:{redis_client.llen("paragraphs")}')

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

'''
# Connect to your Redis server
redis_client = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port = int(os.environ.get('REDIS_PORT', 6379)),
    password=os.environ.get('REDIS_PASSWORD', None),
)


def pop_from_redis():
    # print('call gari rako muji')
    lists_to_pop = ['crawled_data', 'other_data']       # , 'crawled', 'to_crawl']
    popped_data = {}
    for list_name in lists_to_pop:
        if list_name not in popped_data:
            popped_data[list_name] = []
        # print(f'list_name:{list_name}')
        
        # pop until the list is empty
        while redis_client.llen(list_name) > 0:
            # print(f'list_name:{list_name}')
            popped_data[list_name].append(json.loads(redis_client.rpop(list_name)))
    # print("return vayo muji")
     return popped_data
'''

'''
    e.g. format
    {
        'crawled_data': {

        },
        'other_data': {
            
        }
    }

def consumer():
    print('consumer')
    pulled = 0
    while True:
        print(f'{time.time()}: ', end='')
        # Old code using Redis
        paragraphs = pop_from_redis()
        if paragraphs:
            save_to_csv(paragraphs)
            pulled += sum([len(paragraphs[key]) for key in paragraphs.keys()])
            print(f'======consumed: {len(paragraphs)}')     #\n\n current_count:{redis_client.llen("paragraphs")}')
            # print(f'len(paragraphs): {sum([len(paragraphs[key]) for key in paragraphs.keys()])} \n\n paragraphs:{paragraphs}')
        else:
            print('========sleeping for 5 sec.==========')   # No Data
            time.sleep(5)  # sleep for a while before consuming more items

'''


def save_to_csv(data, data_type="crawled_data"):
    
    for data_type, data_items in data.items():
        '''
            data_type: crawled_data, other_data
        '''
        csv_file_path = data_type + ".csv"
        if data_items:
            # field_names = ['paragraph', 'parent_url', 'page_title', 'is_nepali_confidence']
            field_names = data_items[0].keys()
            file_exists = os.path.exists(csv_file_path)
            # print(f'file_exists: {file_exists}')
            # Open the CSV file in append mode
            with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
                # Create a CSV writer object
                csv_writer = csv.DictWriter(csvfile, fieldnames=field_names)
                
                # If the file doesn't exist, write the header
                if not file_exists:
                    csv_writer.writeheader()
                
                try:
                    # # Append the new data
                    # csv_writer.writerows(data_items)
                    if data_type == 'crawled_data':
                        for data_item in data_items:
                            csv_writer.writerow(data_item)
                            
                            sqlite_db = URLDatabase(db_path="urls.db")
                            if sqlite_db.exists("to_crawl",  data_item['parent_url']):
                                # delete the url from to_crawl
                                sqlite_db.delete("to_crawl", data_item['parent_url'])
                                # insert the url to crawled
                                sqlite_db.insert("crawled", data_item['parent_url'])
                            sqlite_db.close()

                except Exception as ex:
                    print(ex)
                    # log the error
                    logging.exception(f'data_type:{data_type} exceptionL {ex}')

def consumer():
    print('consumer')
    pulled = 0
    while True:
        # well formatted date time
        # print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}: ', end='')
        pop_from_mongo()


        
            

        '''
        # Old code using Redis
        paragraphs = pop_from_redis()
        if paragraphs:
            save_to_csv(paragraphs)
            pulled += len(paragraphs)
            print(f'======consumed: {len(paragraphs)}')     #\n\n current_count:{redis_client.llen("paragraphs")}')
            # print(f'len(paragraphs): {len(paragraphs)} \n\n paragraphs:{paragraphs}')
        else:
            print('========sleeping for 5 sec.==========')   # No Data
            time.sleep(5)  # sleep for a while before consuming more items
        '''

to_crawl_cleanup_and_mongo_to_crawl_refill_thread = threading.Thread(target=to_crawl_cleanup_and_mongo_to_crawl_refill)
to_crawl_cleanup_and_mongo_to_crawl_refill_thread.daemon = True
to_crawl_cleanup_and_mongo_to_crawl_refill_thread.start()

display_stats_thread = threading.Thread(target=display_stats)
display_stats_thread.daemon = True
display_stats_thread.start()

backup_thread = threading.Thread(target=backup_crawled_data)
backup_thread.daemon = True
backup_thread.start()

consumer_thread = threading.Thread(target=consumer)
consumer_thread.daemon = True   # daemon threads are forcefully shut down when Python exits and programme waits for non-daemon threads to finish their tasks.
consumer_thread.start()

# Start the thread
# producer_thread.start()
# publisher_thread.start()

# Wait for both threads to finish
# producer_thread.join()
# publisher_thread.join()
# consumer_thread.join()    # waits for consumer_thread to finigh
recover_expired_crawling_thread.join()  # Wait for the thread to finish




















# def producer():
#     import random
#     print('producer')
#     data_paragraphs_copy = [{'paragraph': 'प्रदेश सरकार र निजी क्षेत्रको सहकार्यमा पहाडी तथा हिमाली क्षेत्रमा मनोरञ्जनात्मक तथा साहसिक पर्यटनको लागि हिलस्टेशनहरू विकास गर्न आवश्यक छ भन्दै उनले सांस्कृतिक, धार्मिक, साहसिक, कृषि, स्वास्थ्य तथा खेल पर्यटक आकर्षित गर्दै यस क्षेत्रको मौलिक संस्कृति संरक्षणमा महोत्सवले सहयोग गर्ने विश्वास व्यक्त गरे ।\xa0', 'parent_url': 'https://hamrakura.com/news-details/161504/2023-12-27', 'page_title': 'लोकतन्त्रको आन्दोलन उठाउँदाकै आस्थाबाट निर्देशित छु – राष्ट्रपति पौडेल', 'is_nepali_confidence':'-1914.427728056908'},
#         {'paragraph': 'त्रिवेणी बाहेक अन्य पालिकाबाट कुन-कुन घर परिवारले रकम पाउने भन्ने विवरण नआइसकेकाले ती पालिकाका लागि रकम निकासा भने हुन सकेको छैन ।', 'parent_url': 'https://hamrakura.com/news-details/159820/2023-11-28', 'page_title': 'भूकम्पपीडितको अस्थायी आवासका लागि रकम निकासा', 'is_nepali_confidence':'-800.0689898729324'},
#         {'paragraph': 'निर्वाचित मण्डलले निर्वाचनका सबै प्रक्रिया अघि बढाएपनी सहमतिका लागि शीर्ष नेताहरूले समय मागेकाले निर्वाचन कमिटीले समय दिएको थियो । निर्वाचन कमिटीका संयोजक जगत बहादुर रोकायाले बताए ।','parent_url': 'https://hamrakura.com/news-details/160003/2023-12-01', 'page_title': 'अध्यक्ष मण्डलले घोषणा गरे जिल्ला कमिटी, टिके प्रथा चलाएको रावल पक्षको आरोप', 'is_nepali_confidence':'-1128.5258438587189'},
#         {'paragraph': 'अहिलेसम्म एनसेलले शेयर किनबेच गरेको सम्बन्धमा नेपाल दूरसञ्चार प्राधिकरणले गरेको काम कारबाहीको सम्बन्धमा जानकारी माग्ने पत्र लेख्ने', 'parent_url':'https://hamrakura.com/news-details/161068/2023-12-19', 'page_title': 'एनसेलले राज्यलाई तिर्नुपर्ने कर असुल उपर गर्न सरकारलाई समितिको निर्देशन [भिडियो]', 'is_nepali_confidence':'-800.2218471765518'}
#         ]
#     while True:
#         # add n items to the list
#         n_items = random.randint(0, 3)
#         data_paragraphs.extend(data_paragraphs_copy[:n_items])
#         print(f'produced: {n_items}')

#         # sleep randomly between 0 and 5 seconds
#         time.sleep(random.randint(0, 5))

# def publisher():
#     print('published')
#     while True:
        
#         if data_paragraphs:
#             pushed = 0
#             for paragraph in data_paragraphs:
#                 push_to_redis('crawled_data', json.dumps(data_paragraphs.pop()))
#                 pushed += 1
#             print(f'==-published: {pushed}==-')
#         else:
#             time.sleep(5)  # sleep for a while before producing more items
#         # time.sleep(1)  # sleep for a while before producing more items

# def push_to_redis(list_name, data):
#     redis_client.lpush(list_name, data)

# def load_from_csv(csv_file_path="crawled_data.csv"):
#     data = []
#     # Open the CSV file in read mode
#     with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
#         # Create a CSV reader object
#         csv_reader = csv.DictReader(csvfile)

#         # Read and print each row of data
#         for row in csv_reader:
#             data.append(row)
#             # print(row)
#     return data

# data_paragraphs = []

# # Create producer and consumer threads
# producer_thread = threading.Thread(target=producer)
# publisher_thread = threading.Thread(target=publisher)

# # Start the threads
# producer_thread.start()
# # consumer_thread.start()
# publisher_thread.start()

# # Wait for both threads to finish
# producer_thread.join()
# publisher_thread.join()
# # consumer_thread.join()