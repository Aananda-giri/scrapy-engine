# -----------------
# spider-Test
# -----------------
'''
# Previous crawled_data was in format:
the_crawled_data = [
    {
        'parent_url': 'https://response.url',
        'page_title': "response.css('title::text').get()",
        'paragraph': 'paragraph1',  # list for bulk upload
        # 'is_nepali_confidence': confidence
    },
    {
        'parent_url': 'https://response.url',
        'page_title': "response.css('title::text').get()",
        'paragraph': 'paragraph2',  # list for bulk upload
        # 'is_nepali_confidence': confidence
    },]


#  new crwaled Data format is 
the_crawled_data = {
    'parent_url': 'https://response.url',
    'page_title': "response.css('title::text').get()",
    'paragraphs': ['paragraph1' ,'paragraph2']  # list for bulk upload
    # 'is_nepali_confidence': confidence
}


* Note: key paragraph -> paragraphs
'''

from mongo import Mongo
import json 
mongo = Mongo()

# Test crawled data
the_crawled_data = {
    'parent_url': 'https://response.url',
    'page_title': "response.css('title::text').get()",
    'paragraphs': json.dumps(['paragraph1' ,'para2'])  # list for bulk upload
    # 'is_nepali_confidence': confidence
}

from pymongo.errors import DuplicateKeyError

try:
    mongo.db['test'].insert_one(the_crawled_data)
except DuplicateKeyError:
    pass
except Exception as Ex:
    print(Ex)

list(mongo.db['test'].find({}))

# # Delete data from test collection
# mongo.db['test'].delete_many({})



# -----------------
# server.py Test
# -----------------
import os
import csv
import logging
from mongo import Mongo
local_mongo = Mongo(local=True)
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
        # csv_file_path = data_type + ".csv"
        csv_file_path = 'crawled_data_test.csv'
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



crawled_data = list(mongo.db['test'].find({}))

other_data = []
combined_data = {"crawled_data":crawled_data, "other_data":other_data}
save_to_csv(combined_data)


# loading the csv data
import csv
rows = []
with open('crawled_data_test.csv', 'r', newline='', encoding='utf-8') as input_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            rows.append(row)