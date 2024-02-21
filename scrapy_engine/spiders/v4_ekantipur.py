from .functions import is_social_media_link, is_nepali_language, is_document_link, is_google_drive_link, is_same_domain, is_np_domain, get_resume_urls
import scrapy
import pybloom_live
import scrapy
import dotenv
import json
import os
import redis
import threading
import time
dotenv.load_dotenv()
from scrapy import signals# , Spider
from scrapy.linkextractors import LinkExtractor

class EkantipurSpider(scrapy.Spider):
    name = "ekantipur_v4"
    crawled_data = []
    to_visit = []
    other_data = []
    def publisher(self):
            # print(f'published: {self.crawled_data}')
            # while not self.stop_publisher.is_set() or self.crawled_data or self.to_visit:
            print(f'crawled:{len(self.crawled_data)} redis:{self.redis_client.llen(self.name)}')
            
            # Save crawled data to redis
            # if self.crawled_data:
            #     pushed = 0
            #     for _ in range(len(self.crawled_data)):
            #         self.redis_client.lpush(crawled_data, json.dumps(self.crawled_data.pop()))
            #         pushed += 1
            #     print(f'---published: {pushed}---')
            
            # Save to_visit urls to redis
            if self.to_visit:
                pushed = 0
                for _ in range(len(self.to_visit)):
                    self.redis_client.lpush('to_visit', json.dumps(self.to_visit.pop()))
                    pushed += 1
                print(f'---to_visit: {pushed}---')
            
            if self.other_data:
                pushed = 0
                for _ in range(len(self.other_data)):
                    self.redis_client.lpush('other_data', json.dumps(self.other_data.pop()))
                    pushed += 1
                print(f'---other_data: {pushed}---')
            # time.sleep(1)  # sleep for a while
            
    def __init__(self, *args, **kwargs):
        super(EkantipurSpider, self).__init__(*args, **kwargs)

        self.redis_client = redis.Redis(
            host=os.environ.get('REDIS_HOST', 'localhost'),
            port = int(os.environ.get('REDIS_PORT', 6379)),
            password=os.environ.get('REDIS_PASSWORD', None),
        )
        # self.stop_publisher = threading.Event()
        # Create producer and consumer threads
        # self.publisher_thread = threading.Thread(target=self.publisher)
        # start the threads
        # self.publisher_thread.start()
        
        # self.start_urls = [kwargs.get('start_url')] 
        # using bloom filter to store visited urls for faster lookup
        self.visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)    # pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH means
        

    def parse(self, response):
        links = LinkExtractor(deny_extensions=[]).extract_links(response)
        drive_links = []    # Drive links
        site_links = []     # website links
        
        self.visited_urls.add(response.url)

        # yield every paragraph from current page
        paragraphs = response.css('p::text').getall()
        for paragraph in paragraphs:
            is_nepali, confidence = is_nepali_language(paragraph)
            if is_nepali:
                self.crawled_data.append(
                    {
                        'parent_url': response.url,
                        'page_title': response.css('title::text').get(),
                        'paragraph': paragraph,
                        'is_nepali_confidence': confidence
                    }
                )

        
        # Saving drive links
        for link in links:
            is_drive_link, _ = is_google_drive_link(link.url)     #DriveFunctions.is_google_drive_link(link.url)
            if is_drive_link:
                self.other_data.append({
                    'parent_url': response.url,
                    'url': link.url,
                    'text': link.text,
                    'link_type': "drive_link",
                    'link_description': None
                })
            else:
                is_document, document_type, ignore_doc = is_document_link(link.url)
                if is_document:
                    if not ignore_doc:  # and doc_type != 'image'
                        self.other_data.append({
                            'parent_url': response.url,
                            'url': link.url,
                            'text': link.text,
                            'link_type': "document",
                            'link_description': document_type
                        })
                else:
                    is_social_media, social_media_type = is_social_media_link(link.url)
                    if is_social_media:
                        self.other_data.append({
                            'parent_url': response.url,
                            'url': link.url,
                            'text': link.text,
                            'link_type': "social_media",
                            'link_description': social_media_type
                        })
                    else:
                        site_links.append(link)
        

        # Remove visited url from to_visit set
        self.redis_client.srem('to_visit_set', json.dumps(response.url))

        # Follow Next Page: 
        for site_link in site_links:
            # base_url, crawl_it = should_we_crawl_it(site_link.url)  # self.visited_urls_base,
            if (is_same_domain(response.url, site_link.url) or is_np_domain) and site_link.url not in self.visited_urls:
                # only follow urls from same domain or other nepali domain and not visited yet
                # self.to_visit.append(site_link.url)
                self.redis_client.sadd('to_visit_set', json.dumps(site_link.url))
            
            
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(EkantipurSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider, reason):
        print(f"almost sakyo muji. reason:{reason}")
        self.publisher()    # publish remaining data
        # Wait for both threads to finish
        # self.stop_publisher.set()
        # producer_thread.join()
        # self.publisher_thread.join()
        # consumer_thread.join()

        # This method will be called when the spider is closed
        # self.logger.info('Spider closed. Joining publisher threads.')

        # self.logger.info('Custom threads joined. Spider closing gracefully.')

