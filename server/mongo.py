from .mongo_db_handler import MongoDBHandler
import time

class Mongo():
    def __init__(self):
        self.db_handler = MongoDBHandler(collection_name="scrapy-engine", db_name="scrapy-engine")

    def append_url_crawled(self, url):
        # Add to url_crawled if it does not already exists 
        
        # no need to check if it already exists in url crawled because of unique index in url
        # if not db_handler.exists(field='url', vlaue=url, collection_name='url_crawled'):
        try:
            # Check if it does not already exists
            db_handler.insert_one({'url':url, 'timestamp':time.time()}, collection_name='url_crawled')
        except  Exception as ex:
            pass
        
        # Remove from url_crawling
        db_handler.delete_one(field='url', value=url, collection_name='url_crawling')        
        
        # Remove from url_to_crawl
        db_handler.delete_one(field='url', value=url, collection_name='url_to_crawl')
        
    def append_url_crawling(self, url):
        # Add to url_crawling
        if self.not_crawling_or_not_crawled():
            # Check if url has not already been crawled
            db_handler.insert_one({'url':url, 'timestamp':time.time()}, collection_name='url_crawling')
            
            # Remove from url_to_crawl
            db_handler.delete_one(field='url', value=url, collection_name='url_to_crawl')
            
            return True
        return False
        

    def append_url_to_crawl(self, url):
        # Add to url_crawling
        if self.not_to_crawl_or_not_crawling_or_not_crawled(url):
            db_handler.insert_one({'url':url, 'timestamp':time.time()}, collection_name='url_to_crawl')
            return True
        return False
    
    def not_crawling_or_not_crawled(self, url):
        if not db_handler.exists(field='url', vlaue=url, collection_name='url_crawled'):
            if not db_handler.exists(field='url', vlaue=url, collection_name='url_crawling'):
                return True
        return False
    
    def not_to_crawl_or_not_crawling_or_not_crawled(self, url):
        if not db_handler.exists(field='url', vlaue=url, collection_name='url_crawled'):
            if not db_handler.exists(field='url', vlaue=url, collection_name='url_crawling'):
                if not db_handler.exists(field='url', vlaue=url, collection_name='url_to_crawl'):
                    return True
        return False

    def get_expired_url_crawling(self):
        # crawling urls expire in every 2 hours
        return db_handler.get_items_before_timestamp(timestamp=time.time() - 7200, collection_name='url_crawling')

    def fetch_start_urls(self, number_of_urls_required=15):
        # return [json.loads(url) for url in self.redis_client.srandmember('urls_to_crawl_cleaned_set', number_of_new_urls_required)]
        
        # get urls from to_crawl
        urls = db_handler.get_n_items(collection_name='url_to_crawl', n=number_of_urls_required)
        
        # append them to crawling   -> removes from to_crawl
        for url in urls:
            self.append_url_crawling(url)
        
        # return urls
        return urls


if __name__=="__main__":
    # delete all data and create unique index for field: 'url'
    mongo = Mongo()
    
    collection_names = ['url_crawled', 'url_to_crawl', 'url_crawling']
    for collection_name in collection_names:
        mongo.db_handler.delete_all(collection_name=collection_name)
        mongo.db_handler.db[collection_name].create_index('url', unique=True)