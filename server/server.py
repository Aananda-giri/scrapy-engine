import csv
import json
import os
import redis
import requests
import shutil
import sys
import threading
import time

from mongo import Mongo
mongo = Mongo()
local_mongo = Mongo(local=True)

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w", format="%(asctime)s - %(levelname)s - %(message)s")

from bloom import BloomFilterThread

# load oscar dataset bloom filter (contains urls that are already crawled by oscar_2201 dataset from huggingface)
npberta_oscar_bloom_filter = BloomFilterThread(save_file='npberta_oscar_bloom_filter.pkl')


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
#   ===== Saving configs to mongo online ======
# ======================================================


# Update config in mongo
configs = [
        {'crawl_other_data': False},
        {'crawl_paragraph_data':True},
        # {'some_config':1000}
    ]
# Save configs
mongo.set_configs(configs)

print("\n -----------------------------")
print('\tsaved configs to mongo')
print(" -----------------------------\n")
logging.info("saved configs to mongo")

def run_shub_code():
    '''
    * run 'shub_runner.js' once every 6 hours
    * scrapy hub closes spiders after certain time so this code helps re-initialize spiders.
    '''
    while True:
        print('run_shub_code: started')
        os.system('node shub_runner.js')
        print('-----------------------------------------------')
        print('\t run_shub_code: sleeping 6 hours')
        print('-----------------------------------------------')
        time.sleep(6 * 60 * 60)  # sleep for 6 hours


# ======================================================
# Crawling  ==> to_crawl (if timestamp >2 hours)
# ======================================================


# start_time = time.time()
# number_of_links_crawled_at_start = mongo.collection.count_documents({"status": "crawled"})


def revoke_crawling_url():
    '''
    * Get crawling urls from mongo
    * if they are not in crawled in local_mongo:
        * update status in local_mongo, status=tocrawl
        * delete them from mongo
    
    local_mongo
        `collection.update_many({"url": {"$in": urls}, "status": "crawling"}, {"$set": {"status": "to_crawl"}}, upsert=True)`
        status:crawling <change it to to_crawl>
        status:crawled <leave as it is>
        status:to_crawl <leave as it is>
    
    # Tests:
    import time
    from mongo import Mongo
    local_mongo = Mongo(local=True)
    local_mongo.collection.insert_many([{'url': 'url_crawling', 'status': 'crawling', 'timestamp': time.time()}, {'url': 'url_crawled', 'status': 'crawled', 'timestamp': time.time()}, {'url': 'url_to_crawl', 'status': 'to_crawl', 'timestamp': time.time()}])
    
    # get all urls
    local_mongo.collection.find({})
    
    urls = ['url_crawling', 'url_crawled', 'url_to_crawl', 'new_url']
    local_mongo.collection.update_many({"url": {"$in": urls}, "status": "crawling"}, {"$set": {"status": "to_crawl"}})
    # it should perform following operations:
        * status:crawling <change it to to_crawl>
        
        * insert new_url with status:to_crawl
        * update_many does not support upert=True, insert is not necessary since 
    
    # Get all urls
    list(local_mongo.collection.find({}))
    
    # Delete from mongo
    local_mongo.collection.delete_many({'url': {'$in': urls}})
    '''
    while True:
        print('revoke_crawling_url started')
        timestamp = time.time() - 5 * 60  # 5 minutes
        pipeline = [
            {"$match": {"status": "crawling", "timestamp": {"$lt": str(timestamp)}}},
            # {"$limit": limit}
            # {"$count": "count"}
        ]
        expired_count = list(mongo.collection.aggregate(pipeline + [{"$count": "count"}]))
        if expired_count:
            # expired_count is empty if there are no expired urls
            # expired_count : [{'count': 123}]    # if there are expired urls
            expired_count = expired_count[0]['count']
            if expired_count > 0:
                print(f"revoke_crawling_url: {expired_count} urls expired")
                no_iterations = int(expired_count / 10000) + 1
                for _ in range(no_iterations):
                    expired_crawling_urls = list(mongo.collection.aggregate(pipeline + [{"$limit": 10000}]))
                    
                    # Save to local mongo: by update_many
                    # since url is unique, it would avoid duplicates
                    urls_expired = [entry['url'] for entry in expired_crawling_urls]
                    try:
                        local_mongo.collection.update_many({"url": {"$in": urls_expired}, "status": "crawling"}, {"$set": {"status": "recovered"}})
                    except Exception as ex:
                        pass
                    
                    # Delete from mongo online
                    try:
                        mongo.collection.delete_many({'status': 'crawling', 'url': {'$in': urls_expired}})
                    except Exception as ex:
                        print(ex)
                    print(f"recovered {len(urls_expired)} expired \"crawling\" urls from mongo -> local_mongo")
                    logging.info(f"recovered {len(urls_expired)} expired \"crawling\" urls from mongo -> local_mongo")
        print('revoke_crawling_url: sleeping 5 minutes')
        # sleep for 5 minutes
        time.sleep(5 * 60)

# Create and start the thread as a daemon
revoke_crawling_url_thread = threading.Thread(target=revoke_crawling_url)
revoke_crawling_url_thread.daemon = True
revoke_crawling_url_thread.start()
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
        # crawled_data_count = mongo.db['crawled_data'].count_documents({})
        other_data_count = mongo.db['other_data'].count_documents({})
        error_url_count = mongo.collection.count_documents({"status": "error"})
        # to_crawl_sqlite_count = URLDatabase(db_path="urls.db").count_entries("to_crawl")
        # crawled_sqlite_count = URLDatabase(db_path="urls.db").count_entries("crawled")
        # Nice formatted view for to_crawl, crawled and crawling
        # Formatted output
        print("=================================================")
        print("#  *********** Crawl Queue Status ***********")
        print("=================================================")
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}')
        print(f"\"to_crawl\" (Mongo): {locale.format_string('%d', to_crawl_count, grouping=True)}")
        print(f"\"to_crawl?\" (Mongo by Spider): {locale.format_string('%d', to_crawl_spider_count, grouping=True)}")
        print(f"Error (Mongo): {locale.format_string('%d', error_url_count, grouping=True)}")
        print(f"Crawling (Mongo): {locale.format_string('%d', crawling_count, grouping=True)}")
        # print(f"Crawled_data (Mongo): {locale.format_string('%d', crawled_data_count, grouping=True)}")
        print(f"Other_data (Mongo): {locale.format_string('%d', other_data_count, grouping=True)}")
        # print(f"\"to_crawl\" (Sqlite): {locale.format_string('%d', to_crawl_sqlite_count, grouping=True)}")
        # print(f"\"crawled\" (Sqlite): {locale.format_string('%d', crawled_sqlite_count, grouping=True)}")
        # print(f"Crawled: {locale.format_string('%d', crawled_count, grouping=True)}")
        
        
        # Get mongo stats
        # stats = mongo.db.command("dbstats")
        # Print the stats
        # print("DB Size: ", stats['dataSize']/(1024*1024))
        # print("Storage Size: ", stats['storageSize']/(1024*1024))
        # print("Mongo Free Storage Space: ", stats['totalFreeStorageSize']/(1024*1024), end="\n-----------------------------------------------\n")
        
        # Crawling Rate
        # newly_crawled = crawled_count - number_of_links_crawled_at_start
        # time_taken = time.time() - start_time
        # crawling_rate = newly_crawled / time_taken
        # print(f"Crawling Rate: {locale.format_string('%d', crawling_rate, grouping=True)} links/sec")
        # expected_time_to_crawl = to_crawl_count / (crawling_rate if crawling_rate > 0 else 0.0000001)
        # print(f"Crawling Rate: {crawling_rate} links/sec")
        # print(f"Expected Time to Crawl: {locale.format_string('%d', expected_time_to_crawl/(60*60*24), grouping=True)} days")
        
        # Get Crawled File Size
        print(f"Size \"crawled_data.csv\": {os.path.getsize('crawled_data.csv')/(1024*1024) if os.path.exists('crawled_data.csv') else 0} MB")
        print(f"Size of \"other_data.csv\": {os.path.getsize('other_data.csv')/(1024*1024) if os.path.exists('other_data.csv') else 0} MB")
        print(f"Size of urls.db: {os.path.getsize('urls.db')/(1024*1024) if os.path.exists('urls.db') else 0} MB")
        print("===============================================")
        # -----------------------------------------------------------------------

        # Sleep for 1 minute
        print('display_stats: sleeping 1 minute')
        time.sleep(60)




def to_crawl_cleanup_and_mongo_to_crawl_refill():
    '''
        * `to_crawl?` are next links given by crawler. add to_carwl? from online_mongo to local_mongo if url is not crwaled
        * This thread will run once every 1.5 hours? 
        - lets not allowmake it sleep, the delays from operations should be enouogh
    '''
    # ------------------------------------------------------------------------------------------------
    # mograte "to_crawl?" from online mongo -> local_mongo
    # -----------------------------------------------------
    '''
    * Get to_crawl? from online mongo
    * loop over each url:
        * insert if url does not exists locally
        i.e. insert_many with ordered=False should do
        ```
            local_mongo.collection.insert_many(
                [
                    {
                        'url': entry['url'],
                        'status': 'to_crawl',
                        'timestamp': entry['timestamp']
                    } 
                    for entry in entries
                ],
                ordered=False)
        ```
        
        ```
            # Test
            # pre existing data
            local_mongo.collection.insert_one({'url': 'existing_url_with_status_crawled', 'status': 'crawled', 'timestamp': time.time()})

            # new data
            data = [
                {'url': 'existing_url_with_status_crawled', 'status': 'to_crawl', 'timestamp': time.time()},
                {'url': 'new_url_with_status_to_crawl', 'status': 'to_crawl', 'timestamp': time.time()}
            ]

            try:
                local_mongo.collection.insert_many(data, ordered=False)
            except Exception as bwe:
                pass

            # Get all data
            list(local_mongo.collection.find({}))

            # Delete from local mongo
            local_mongo.collection.delete_many({'url': {'$in': ['existing_url_with_status_crawled', 'new_url_with_status_to_crawl']}})
        ```
    * delete from online mongo
    '''
    print("mongo_to_crawl_refill started")
    logging.info("mongo_to_crawl_refill started")
    while True:    
        # +1 to avoid 0 division error and int() returns floor value. e.g. 1.9 -> 1
        to_crawl_count = mongo.collection.count_documents({"status": 'to_crawl?'})
        if to_crawl_count > 0:
            n_iterations = int(to_crawl_count/10000) + 1
            for _ in list(range(n_iterations)):
                # print(_)
                # 10000 at a time
                
                # Get all urls from "to_crawl?"
                entries = mongo.collection.find({"status": 'to_crawl?'}).limit(10000)
                entries = list(entries)
                # making two lists entries and entries_non_crawled in order to save entries_non_crawled to local_mongo and delete all entries from online_mongo
                entries_non_crawled = [entry for entry in entries if entry['url'] not in npberta_oscar_bloom_filter]
                # Save to local mongo
                # print('pre-insert')
                start_time = time.time()
                try:
                    _ = local_mongo.collection.insert_many(
                            [
                                {
                                    'url': entry['url'],
                                    'status': 'to_crawl',
                                    'timestamp': entry['timestamp']
                                } 
                                for entry in entries_non_crawled
                            ],
                            ordered=False)
                except Exception as e:
                    pass
                # print(f'inserted. time:{time.time()-start_time}, rate:{len(entries)/(time.time()-start_time)}')
                # Delete from online mongo
                start_time = time.time()
                try:
                    _ = mongo.collection.delete_many(
                        {
                            'status': 'to_crawl?',
                            'url': {'$in': [entry['url'] for entry in entries]}
                        })
                except Exception as e:
                    pass
                # print(f'deleted. time:{time.time()-start_time}, rate:{len(entries)/(time.time()-start_time)}')
                # print('deleted')
            print(f"migrated <= {n_iterations*10000} \"to_crawl?\" from online_mongo to local_mongo")
            logging.info(f"migrated {n_iterations*10000} \"to_crawl?\" from online_mongo to local_mongo")
        else:
            print('no to_crawl? to migrate')
            logging.info('no to_crawl? to migrate')
        # ------------------------------------------------------------------------------------------------------------------------
        # mongo_to_crawl_refill
        # -----------------------
        '''
            * To maintain 50,000 - 60,000 to_crawl urls in online_mongo
            * Get urls with `status=to_crawl` local_mongo
            * insert_many to online mongo
            * update_many `status:crawling` to in local_mongo

            # Test
            ```
                # pre existing data
                local_mongo.collection.insert_one({'url': 'existing_url_with_status_to_crawl', 'status': 'to_crawl', 'timestamp': time.time()})
                local_mongo.collection.insert_one({'url': 'existing_url_with_status_crawling', 'status': 'crawling', 'timestamp': time.time()})
                local_mongo.collection.insert_one({'url': 'existing_url_with_status_crawled', 'status': 'crawled', 'timestamp': time.time()})

                # get urls from local_mongo
                to_crawl_entries = list(local_mongo.collection.find({"status": 'to_crawl'}).limit(10))

                # Insert to online mongo

                # Update status in local_mongo
                # no need to check status, since we are already filtering with status
                local_mongo.collection.update_many(
                    {
                        'url': {'$in': [entry['url'] for entry in to_crawl_entries]},
                    }, {'$set': {'status': 'crawling'}})

                # get all data
                list(local_mongo.collection.find({}))
                # Delete data
                local_mongo.collection.delete_many({})
            ```
        '''
        online_to_crawl_count = mongo.collection.count_documents({"status": 'to_crawl'})
        required_to_crawl_count = 50000 - online_to_crawl_count
        if required_to_crawl_count > 0:
            # refill 10000 at a time
            # to avoid max. sized reached error of mongo        
            print(f'to refill: {required_to_crawl_count} to_crawl urls')
            no_iterations = int(required_to_crawl_count/10000) + 1
            for _ in range(no_iterations):
                # Get urls from local_mongo
                to_crawl_entries = list(local_mongo.collection.find({"status": 'to_crawl'}).limit(10000))
                
                # Insert to online mongo
                try:
                    mongo.collection.insert_many(
                        [
                            {
                                'url': entry['url'],
                                'status': 'to_crawl',
                                'timestamp': entry['timestamp']
                            } 
                            for entry in to_crawl_entries
                        ],
                        ordered=False)
                    print(f"attempted inserting {no_iterations*10000} to_crawl to online_mongo")
                    logging.info(f"attempted inserting {no_iterations*10000} to_crawl to online_mongo")
                except Exception as bwe:
                    print(f'failed to refill {required_to_crawl_count} : reason: {ex}')
                    logging.info(f'failed to refill {required_to_crawl_count} : reason: {ex}')
                
                # Update status in local_mongo
                local_mongo.collection.update_many(
                        {'url': {'$in': [entry['url'] for entry in to_crawl_entries]},},
                        {'$set': {'status': 'crawling'}}
                    )
            
            
            
        # -------------------------------------------------------------------------------------------------------------------------
        # Save error data to csv file from online_mongo
        # ----------------------------------------------
        error_count = mongo.collection.count_documents({'status': 'error'})
        if error_count > 0:
            no_iterations = int(error_count/10000) + 1
            for _ in range(no_iterations):
                error_data = list(mongo.collection.find({'status': 'error'}).limit(10000))
                if error_data:
                    formatted_for_csv = [{
                            'url': error['url'],
                            'timestamp': error['timestamp'],
                            'status': error['status'],
                            'status_code': error['status_code'] if 'status_code' in error else None,
                            'error_type': error['error_type']
                        } for error in error_data]
                    # Append error data to csv
                    csv_file_path = 'error_data.csv'
                    file_exists = os.path.exists(csv_file_path)
                    with open(csv_file_path, 'a') as csvfile:
                        fieldnames = ['url', 'timestamp', 'status', 'status_code', 'error_type']
                        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if not file_exists:
                            print(f"creating csv file: {csv_file_path}")
                            # Write header only if file is empty
                            csv_writer.writeheader()
                        # else:
                        #     print(f'csv file: \"{csv_file_path}\" exists')
                        csv_writer.writerows(formatted_for_csv)
                    
                    print(f"migrated {len(formatted_for_csv)} \"error?\" from mongo ->  {csv_file_path}", end="\n\n")
                    # Delete from mongo
                    mongo.collection.delete_many({'url': {'$in': [error['url'] for error in formatted_for_csv]}, 'status': 'error'})
        # ------------------------------------------------------------------------------------------------------------------------
        print('to_crawl_cleanup_and_mongo_to_crawl_refill: sleeping 1 minute')
        time.sleep(10)  # sleep for 10 seconds



def backup_crawled_data():
    # ---------------------------------------------------------------------------
    ### Backup file named crawled_data.csv & other_data.csv after every 12 hours
    # ---------------------------------------------------------------------------
    while True:
        print('backup thread started')
        
        # Backup once for every 12 hours
        print('backup_thread: sleeping 12 hours')
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
            print(f'backup created: crawled_data_backup_{max_index+1}.csv')
            logging.info(f'backup created: crawled_data_backup_{max_index+1}.csv')
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
        
        # Keep at most 3 copies of crawled_data
        for index in range(max_index-2):
            # Delete backup copies with index <= max_index - 2
            try:
                os.remove(f'other_data_backup_{index}.csv')
                print(f'removed other_data_backup_{index}.csv')
            except Exception as Ex:
                pass
                # print(Ex)
            
            
            # Delete backup copies with index <= max_index - 2
            try:
                os.remove(f'crawled_data_backup_{index}.csv')
                print(f'removed crawled_data_backup_{index}.csv')
            except Exception as Ex:
                pass
                # print(Ex)

# ======================================================
# Save data from online mongo to csv file
# ======================================================

def save_to_csv(data, data_type="crawled_data"):
    '''
        * Save the crawled_data to 'csv' file
        * update_many status of parent_url to 'crawled' in local_mongo
        
        * data_item['parent_url'] is data in format: url <str>
        
        # Test
        ```
            # Pre existing data
            local_mongo.collection.insert_many([
                {'url': 'url_crawling', 'status': 'crawling', 'timestamp': time.time()},
                {'url': 'url_crawled', 'status': 'crawled', 'timestamp': time.time()},
                {'url': 'url_to_crawl', 'status': 'to_crawl', 'timestamp': time.time()},
                {'url': 'some_other_url', 'status': 'to_crawl', 'timestamp': time.time()}
            ])

            entries = ['url_crawling', 'url_crawled', 'url_to_crawl', 'new_url']

            # update status of parent_url to 'crawled' in local_mongo                                       
            local_mongo.collection.update_many(
                {'url':{'$in': entries}},
                {'$set': {'status': 'crawled'}},
            )

            # Display records
            print(list(local_mongo.collection.find({})))
            
            # Delete from local_mongo
            local_mongo.collection.delete_many({})
        ```
    '''
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
                    # Get all unique crawled_urls
                    entries = list(set([data_item['parent_url'] for data_item in data_items]))
                    
                    # update status of parent_url to 'crawled' in local_mongo
                    local_mongo.collection.update_many(
                        {'url':{'$in': entries}},
                        {'$set': {'status': 'crawled'}},
                    )
                    
                    # Save crawled data to csv file
                    csv_writer.writerows(data_items)
                except Exception as ex:
                    print(ex)
                    # log the error
                    logging.exception(f'data_type:{data_type} exceptionL {ex}')

def notify_idle(bot_message=None):
    if not bot_message:
        bot_message = "The crawler's taking a nap. Time to wake it up and get it moving!"
    bot_message = str(round(os.path.getsize('crawled_data.csv')/(1024*1024), 2) if os.path.exists('crawled_data.csv') else 0) + 'Mb: ' + bot_message
    bot_token = os.environ.get('TELEGRAM_TOKEN')
    user_id = os.environ.get('TELEGRAM_USER_ID')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + user_id + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    print(f'idle: notified user:{response}')
    # return response.json()

def crawled_data_consumer():
    
    '''
    *  invoke `notify_idle()` function if `save_to_csv()` is not called for 5 minutes,
    '''
    print('crawled_data_consumer: started')
    last_save_time = time.time()  # Track the last time save_to_csv() was called
    notified_once = False         # To notify only once in each idle peroid
    while True:
        # Empty crawled_data and other_data from online mongo
        crawled_count = mongo.db['crawled_data'].count_documents({})
        other_count = mongo.db['other_data'].count_documents({})
        max_data_count = max(crawled_count, other_count)

        if max_data_count > 0:
            no_iterations = int(max_data_count / 10000) + 1
            for _ in range(no_iterations):
                crawled_data = list(mongo.db['crawled_data'].find({}).limit(10000))
                other_data = list(mongo.db['other_data'].find({}).limit(10000))

                if crawled_data or other_data:
                    combined_data = {"crawled_data": crawled_data, "other_data": other_data}

                    # Save to .csv file
                    save_to_csv(combined_data)
                    last_save_time = time.time()  # Update the last save time
                    if notified_once:
                        # it would be nice to notify user when the crawler is back online
                        notify_idle("The crawler’s awake and on the move! It’s working hard to fetch the latest data.")
                        notified_once = False
                    print(f"crawled_data: saved {len(crawled_data)} crawled_data and {len(other_data)} other_data to csv file")

                    # Delete multiple data by id
                    mongo.db['crawled_data'].delete_many({"_id": {"$in": [data['_id'] for data in crawled_data]}})
                    mongo.db['other_data'].delete_many({"_id": {"$in": [data_ot['_id'] for data_ot in other_data]}})

        # Check if it's been more than 5 minutes since the last save
        if (time.time() - last_save_time > 5 * 60) and not notified_once:
            print(f'\n =========================================================== ')
            print(f'\t\t crawler is idle. notifying user \t\t')
            print(f'\n =========================================================== \n ')
            notify_idle()
            last_save_time = time.time()  # Reset last save time after notifying
            notified_once = True

        else:
            sleep_duration = 30  # Sleep for 30 seconds
            print(f'crawled_data_consumer: sleeping {sleep_duration} sec.')
            time.sleep(sleep_duration)



# crontab seems better option for this
# shub_thread = threading.Thread(target=run_shub_code)
# shub_thread.daemon = True
# shub_thread.start()
# notify_idle('why tf is it not notifying?')
to_crawl_cleanup_and_mongo_to_crawl_refill_thread = threading.Thread(target=to_crawl_cleanup_and_mongo_to_crawl_refill)
to_crawl_cleanup_and_mongo_to_crawl_refill_thread.daemon = True
to_crawl_cleanup_and_mongo_to_crawl_refill_thread.start()

display_stats_thread = threading.Thread(target=display_stats)
display_stats_thread.daemon = True
display_stats_thread.start()

backup_thread = threading.Thread(target=backup_crawled_data)
backup_thread.daemon = True
backup_thread.start()

crawled_data_consumer_thread = threading.Thread(target=crawled_data_consumer)
crawled_data_consumer_thread.daemon = True   # daemon threads are forcefully shut down when Python exits and programme waits for non-daemon threads to finish their tasks.
crawled_data_consumer_thread.start()

# Start the thread
# producer_thread.start()
# publisher_thread.start()

# Wait for both threads to finish
# producer_thread.join()
# publisher_thread.join()
# consumer_thread.join()    # waits for consumer_thread to finigh
revoke_crawling_url_thread.join()  # Wait for the thread to finish






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