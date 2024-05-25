import os
import time
from pymongo.server_api import ServerApi

from dotenv import load_dotenv
load_dotenv()

# Creating mangodb
from pymongo import MongoClient

class Mongo():
    def __init__(self, db_name='scrapy-engine', collection_name="urls-collection"):
        uri = f"mongodb+srv://{os.environ.get('mongo_username')}:{os.environ.get('mongo_password')}@scrapy-engine.cnaygdb.mongodb.net/?retryWrites=true&w=majority&appName=scrapy-engine"
        # uri = f"mongodb+srv://{os.environ.get('user_heroku')}:{os.environ.get('pass_heroku')}@cluster0.dgeujbs.mongodb.net/?retryWrites=true&w=majority"
        
        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        client = MongoClient(uri, server_api=ServerApi('1'))
        
        # Create the database for our example (we will use the same database throughout the tutorial
        self.db = client[db_name]
        self.collection = self.db[collection_name]  # using single collection for all urls
        

    def append_url_crawled(self, url):
        try:
            # Try inserting url
            self.collection.insert_one({'url':url, 'timestamp':time.time(), 'status':'crawled'})
        except  Exception as ex:
            # url exists: change status to 'crawled'
            self.collection.update_one({'url':url}, {'$set': {'status':'crawled'}})
        
    def append_url_crawling(self, url):
        try:
            # Try inserting url
            self.collection.insert_one({'url':url, 'timestamp':time.time(), 'status':'crawling'})
            return True # inserted
        except  Exception as ex:
            # modify to_crawl to crawling
            result = self.collection.update_one(
                {'url': url, 'status': {'$in': ['to_crawl']}},
                {'$set': {'status':'crawling'}}, 
            )
            if result.modified_count > 0:
                # print("url was in to_crawl and now changed to crawling")
                return True
            else:
                # print("it is either already crawling or crawled")
                return False
    
    def append_url_to_crawl(self, url):
        try:
            # Try inserting url
            self.collection.insert_one({'url':url, 'timestamp':time.time(), 'status':'to_crawl'})
            return True # inserted
        except  Exception as ex:
            return False    # url exists

    def append_error_data(self, data):
        # Delete url if it's status is either 'to_crawl' or crawling
        # if status is crawled, try/except should avoid creating error url
        results = list(self.collection.find({'url':data['url'], 'status': {'$in': ['to_crawl', 'crawling']}}))
        if results:
            self.collection.delete_one({'_id':results[0]['_id']})

        try:
            # Try inserting url
            self.collection.insert_one(data)
            return True # inserted
        except  Exception as ex:
            print(ex)
            return   # error data exists

    def recover_expired_crawling(self, created_before=7200):
        def convert_from_crawling_to_to_crawl(urls):
            for url in urls:
                self.collection.update_one(
                    {'_id':url['_id'], 'status': {'$in': ['crawling']}},
                    {'$set': {'status':'to_crawl'}}
                    )

        # get items with status 'crawling' and before timestamp 2 hours (7200)
        timestamp = time.time() - created_before  # 2 hours ago
        expired_crawling_urls = list(self.collection.find({'status':'crawling', 'timestamp': {'$lt': timestamp}}))
        convert_from_crawling_to_to_crawl(expired_crawling_urls)

    
    def fetch_start_urls(self, number_of_urls_required=10):
        # return [json.loads(url) for url in self.redis_client.srandmember('urls_to_crawl_cleaned_set', number_of_new_urls_required)]
        
        # Get all entries with  status 'to_crawl'
        urls = list(self.collection.find({'status':'to_crawl'}).limit(number_of_urls_required))

        ## update status to crawling
        for url in urls:
            self.collection.update_one({'_id':url['_id']}, {'$set': {'status':'crawling'}})
        
        return urls
    def fetch_all(self):
        return list(self.collection.find())



if __name__=="__main__":
    # delete all data and create unique index for field: 'url'
    mongo = Mongo()
    
    collection_names = ['url_crawled', 'url_to_crawl', 'url_crawling']
    for collection_name in collection_names:
        mongo.db_handler.delete_all(collection_name=collection_name)
        mongo.db_handler.db[collection_name].create_index('url', unique=True)