from pathlib import Path

import scrapy

import csv
import dotenv
import json
import os
import redis
# import threading
import pybloom_live
import time
dotenv.load_dotenv()
from scrapy import signals
class QuotesSpider(scrapy.Spider):
    name = "quotes"
    crawled_data = []
    to_visit = []
    def publisher(self):
        # print(f'published: {self.crawled_data}')
        # while not self.stop_publisher.is_set() or self.crawled_data or self.to_visit:
        print(f'crawled:{len(self.crawled_data)} redis:{self.redis_client.llen(self.name)}')
        
        # Save crawled data to redis
        if self.crawled_data:
            pushed = 0
            for _ in range(len(self.crawled_data)):
                self.redis_client.lpush(self.name, json.dumps(self.crawled_data.pop()))
                pushed += 1
            print(f'---published: {pushed}---')
        
        # Save to_visit urls to redis
        # if self.to_visit:
        #     pushed = 0
        #     for _ in range(len(self.to_visit)):
        #         self.redis_client.lpush('to_visit', self.to_visit.pop())
        #         pushed += 1
        #     print(f'---to_visit: {pushed}---')
        
        # time.sleep(1)  # sleep for a while
    def __init__(self):
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

        start_urls = [
                "https://quotes.toscrape.com/page/1/",
                # "https://quotes.toscrape.com/page/2/",
            ]
        self.visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)    # pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH means
        self.redis_client.sadd('to_visit_quotes_set', *[json.dumps(url) for url in start_urls])
        # Get 5 random urls from redis set
        self.start_urls = self.fetch_start_urls()
        print(f'\n\nstart_urls: {self.start_urls}')

        # url = "https://quotes.toscrape.com/"
        # tag = getattr(self, "tag", None)
        # if tag is not None:
        #     url = url + "tag/" + tag
        # yield scrapy.Request(url, self.parse)
    def fetch_start_urls(self):
        return [json.loads(url) for url in self.redis_client.srandmember('to_visit_quotes_set', 15)]
    def parse(self, response):
        for quote in response.css("div.quote"):
            self.crawled_data.append(
                {
                    "text": quote.css("span.text::text").get(),
                    "author": quote.css("small.author::text").get(),
                    "url": response.url,
                }
            )
            
            yield {
                "text": quote.css("span.text::text").get(),
                "author": quote.css("small.author::text").get(),
            }
        next_page = response.css("li.next a::attr(href)").get()

        
        self.publisher()
        if next_page is not None:
            self.redis_client.sadd('to_visit_quotes_set', json.dumps(response.urljoin(next_page)))
            # self.to_visit.append(response.urljoin(next_page))
            # Save next_page to Redis and remove the crawled URL from Redis
            # yield response.follow(next_page, self.parse)
         
        
        
        
        # else:
        #     # wait until the next batch of URLs are available
        #     time.sleep(2)

        #     next_urls = self.fetch_start_urls()
        # # Remove visited url from to_visit set -> Now removed only  by back-end
        if 'redirect_urls' in response.request.meta:
            current_url = response.request.meta['redirect_urls'][0]
        else:
            current_url = response.request.url
        self.visited_urls.add(response.url)
        # self.redis_client.srem('to_visit_quotes_set', json.dumps(current_url))
        # Fetch the next batch of URLs if available
        next_urls = self.fetch_start_urls()
        if next_urls:
            for url in next_urls:
                if url not in self.visited_urls:
                    print(f'\n\n update visited: {url}\n')
                    self.visited_urls.add(url)
                    # print(url)
                    yield scrapy.Request(url, callback=self.parse)
        
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(QuotesSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_closed(self, spider, reason):
        print("almost sakyo muji.")
        self.publisher()
        # Wait for both threads to finish
        # self.stop_publisher.set()
        # producer_thread.join()
        # self.publisher_thread.join()
        # consumer_thread.join()

        # This method will be called when the spider is closed
        self.logger.info('Spider closed. Joining publisher threads.')

        self.logger.info('Custom threads joined. Spider closing gracefully.')

        # Re-start the spider
        # process = CrawlerProcess(get_project_settings())
        # process.crawl(QuotesSpider)
        # process.start()

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl(QuotesSpider)
    process.start()