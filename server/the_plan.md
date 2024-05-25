# Scrapy is too slow (0-10 pages per minute):
Settings:
```
COOKIES_ENABLED=False   # Enabled by default
CONCURRENT_REQUESTS     # 100
```

[ ] Merge V1 Data with V2 Data.
[ ] Look for new updates from already crawled base urls like new daily news.


[X] is_nepali_paragraph() before saving paragraph

[ ] Remove same urls from mongo that are saved as different urls due to different fragments

[X] Fragmented urls are the one. 
    e.g. URLs: "https://www.bbc.com/nepali/news-63445425" and "https://www.bbc.com/nepali/news-63445425#content" are saved as two different URLs.

    solution: remove fragments from url before storing/yielding to crawl
    ```
    from urllib.parse import urlparse

    def remove_fragments_from_url(url):
        parsed_url = urlparse(url)
        return parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path

    # Example usage
    url1 = "https://www.bbc.com/nepali/news-63445425"
    url2 = "https://www.bbc.com/nepali/news-63445425#content"

    base_url1 = get_base_url(url1)
    base_url2 = get_base_url(url2)
```
[ ] Identify patterns like english.deshsanchar.com contains news in english

[ ] http://csgrants.gov.np/Home/ViewNoticeNew : AttributeError: Response content isn't text (it is pdf)
[X] is_same_domain(response.url, site_link.url) or    # it is a problem because attempting to crawl bbc.com/nepali can lead to bbc.com
    * remove all bbc.com urls not containing bbc.com/nepali
[ ] * remove all bbc.com data from crawled_data for url not starting with bbc.com/nepali (it is news in english downloaded due to an error)
[X] mongo to store error urls (urls with response 403)
[X] scrapy remove bloom function: Scrapy have built in method (defined inside Scrapy.Request() with attribute dont_filter=False) to not visit visited url

# Mongo Single Collection
- Now it does make sense to use single collection with unique index to url
    ### Fields
        - status: one of ['crawled', 'crawling', 'to_crawl']
        - timestamp: time.time()
        - url: <url>
    ### Advantages:
        - Dont have to delete data from 'crawling' or 'to_crawl' while appending to crawled, we just have to  change status to crawled.

# Scrapy before yield Crawl Request
[X] if self.mongo.append_url_crawling(site_link.url):
        # yield url to crawl new urls_to_crawl by itself
        yield scrapy.Request(site_link.url, callback=self.parse)

[X] Check url not in url_crawled before starting crawling or before yielding it fro crawling

# Why use two databases (MongoDB + Redis)?
    * because mongodb has only 500MB storage (can't store crawled data). 
    * Redis is (smaller but) faster than MongoDb if we are to use it as temporary storage untill ec2 instance pops data from it.
    * why not api in central server?: concurrent read/write combined with multiple workers, would be easier to handle via redis, instead of free ec2 instance, where discord bot is running on the other side.

[X] first step of def parse(): check mongo if url is already crawled or url_crawling

[X] * crawl headings along with paragraphs.

[X] * remove from (db.to_crawl or db.crawling) once (Crawled_data or other_date).parent_url is obtained
[ ] * collect code and run using threading.

# MongoDb or Sqlite
    ## data_types:
        * url_crawled
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
[X] * url_crawling: if in crawling for longer than 2 hour then put it into to_crawl
        * get_urls_if_timeout(time=1hr)
    * urls_crawled: list or set
        * contains(url) -> True/False
        * Append(url)   -> add url

[ ] * log the errors
[X] * url_to_crawl, url_crawling, urls_to_crawl_cleaned_set->updated by server
[ ] * use crawled_data to update urls_to_crawl_cleaned_set i.e. make it 'url_crawled' in mongo.
[X] * remove is_nepali_confidence

# redis Data types
* to_visit_set -> urls_to_crawl_cleaned_set   : set of urls sent by central server
    * In v2 only seed urls are set by central server. After that they are managed using mongodb by worker.

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
[X] BBC.com : Only follow /nepali

## Potential problems
* central server failure
* what if redis is full