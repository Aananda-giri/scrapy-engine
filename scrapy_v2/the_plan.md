# Why use two databases (MongoDB + Redis)?
* because mongodb has only 500MB storage (can't store crawled data). 
* Redis is (smaller but) faster than MongoDb if we are to use it as temporary storage untill ec2 instance pops data from it.
* why not api in central server?: concurrent read/write combined with multiple workers, would be easier to handle via redis, instead of free ec2 instance, where discord bot is running on the other side.

[ ] first step of def parse(): check mongo if url is already crawled or url_crawling

[ ] * crawl headings along with paragraphs.

* remove from (db.to_crawl or db.crawling) once (Crawled_data or other_date).parent_url is obtained
* collect code and run using threading.
# MongoDb or Sqlite
    ## data_types:
        * urls_crawled
        * url_crawling
        * url_to_crawl

    [X] def save_to_db(url_type, url):    
        if not db_handler.exists(field='url', vlaue=url, collection_name='url_crawled'):
            # if url not in db.crawled():
            # url not already crawled
            if url_type == 'url_crawling':
                    db_handler.insert_one({'url':url, 'timestamp':time.time()}, collection_name='url_crawling')
                    db_handler.delete_one(field='url', value=url, collection_name='url_to_crawl')
                    # db.sadd('url_crawling', json.dumps(url))    # add to crawling
                    # db.srem('url_to_crawl', url)                # remove if exist in to_crawl
            elif url_type == 'url_to_crawl':
                if not db_handler.exists(field='url', vlaue=url, collection_name='url_crawling'):
                    # url not already crawling
                    db_handler.insert_one({'url':url, 'timestamp':time.time()}, collection_name='url_to_crawl')
                    # if url not in db.crawling():
                    # db.sadd('url_to_crawl', json.dumps(url))    # add url in to_crawl

    * url_to_crawl
[ ] * url_crawling: if in crawling for longer than 1 hour then put it into to_crawl
        * get_urls_if_timeout(time=1hr)
    * urls_crawled: list or set
        * contains(url) -> True/False
        * Append(url)   -> add url

[ ] * log the errors
[ ] * url_to_crawl, url_crawling, urls_to_crawl_cleaned_set->updated by server
[ ] * use crawled_data to update urls_to_crawl_cleaned_set
[ ] * remove is_nepali_confidence

# redis Data types
* to_visit_set -> urls_to_crawl_cleaned_set   : set of urls sent by central server

* self.redis_client.lpush('crawled_data', json.dumps(
                    {
                        'parent_url': response.url,
                        'page_title': response.css('title::text').get(),
                        'paragraph': paragraph,
                        'is_nepali_confidence': confidence
                    }
                ))


* self.redis_client.lpush('other_data', json.dumps(
                    {
                        'parent_url': response.url,
                        'url': link.url,
                        'text': link.text,
                        'link_type': <one of ["drive_link", "document", "social_media"]> 
                        'link_description': None
                    }
                ))

* `url_to_crawl` -> url_to_crawl: sent by worker
* `url_crawling` -> url_crawling: sent by worker

# Get more start_urls at the end of each parse to keep worker busy 
    * no_of_new_urls_required = self.crawler.engine.settings.get('CONCURRENT_REQUESTS') - len(self.crawler.engine.slot.inprogress)
    * fetch_start_urls(no_of_new_urls_required)

# What do you think:

I am using Scrapy crawler.

Worker gets urls_to_crawl as seed urls from central_server.

it crawls them and generate crawled_data and new urls_to_crawl.

now, it have two choices with new urls_to_crawl: it can either crawl new urls_to_crawl by itself or it can send new urls_to_crawl to central server.

can we make it crawl new urls_to_crawl if current number of CONCURRENT_REQUESTS is less than threshold else send new urls_to_crawl to central server?


## Method1: get current number of concurrent_requests
    * `len(self.crawler.engine.slot.inprogress)`    # returns active number of concurrent processes
    * self.crawler.engine.settings.get('CONCURRENT_REQUESTS')   # returns CONCURRENT_REQUESTS value set in settings.py

    if len(self.crawler.engine.slot.inprogress) > self.crawler.engine.settings.get('CONCURRENT_REQUESTS'):
        # send new urls_to_crawl to central server.
    else:
        # yield url to crawl new urls_to_crawl by itself
        # Send crawling notice to server

## Method2: use a variable 
def __init__(self):
        self.concurrent_requests = 0
def parse(self, response):
    self.concurrent_requests += 1
    # Crawl
    # yield new_url_crawling if  self.concurrent_requests < threshold
    # yield new_url_to_crawl otherwise
    self.concurrent_requests -= 1


send crawling and to_crawl

## Distributed Worker:
    - unused function: `publisher()` in spiders/master_slave.py
    - thought: # Initially I was thinking of using threads to upload to redis then again scrapy already have concurrent_processes so this function seemed un-necessary. Lets see the performance without threads-within-threads we'll use this if necessary.

## Central Server: 
    - have seed_url as urls_to_crawl
    - distribute seed_urls to workers
    - receive new urls_to_crawl
    - remove crawled_url from urls_to_crawl

    - store/reterieve-from sqlite: metadata like urls_crawled, urls_to_crawl



## Special cases:
[ ] BBC.com : Only follow nepali

## Potential problems
* central server failure
* what if redis is full