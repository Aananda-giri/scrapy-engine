from .functions import  load_env_var_in_google_colab, remove_fragments_from_url, is_same_domain

import scrapy
import dotenv
import json

# import redis
# import threading
import time

# from scrapy import signals# , Spider
from scrapy.linkextractors import LinkExtractor

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError, TimeoutError

import sys

sys.path.append('../server/../')
from .mongo import Mongo
from .pickle_utils import PickleUtils


class WorkerSpider(scrapy.Spider):
    name = "worker_spider"
    # crawled_data = []
    # to_visit = []
    # other_data = []
    
            
    def __init__(self, *args, **kwargs):
        # load environment variables if running in google colab
        load_env_var_in_google_colab()
        # dotenv.load_dotenv("server/.env")

        # super(EkantipurSpider, self).__init__(*args, **kwargs)
        self.mongo = Mongo()
        
        # ----------------------------------------------------------------------------
        # config variable from mongo db to decide whether or not to crawl other_data
        print('------------------------------------')
        
        # Get configs
        configs = self.mongo.get_configs()
        # # default: Do not Crawl other data
        # self.crawl_other_data = configs['crawl_other_data'] if 'crawl_other_data' in configs else False
        # print(f'config.crawl_other_data: {self.crawl_other_data}')
        
        # default: Crawl paragraph data
        # self.crawl_paragraph_data = configs['crawl_paragraph_data'] if 'crawl_paragraph_data' in configs else True
        # print(f'config.crawl_paragraph_data: {self.crawl_paragraph_data}')
        print('------------------------------------')
        
        
    def start_requests(self):
        # get number of concurrent requests from settings
        n_concurrent_requests = self.crawler.engine.settings.get('CONCURRENT_REQUESTS')
        start_urls = [remove_fragments_from_url(data_item['url']) for data_item in self.mongo.fetch_start_urls(n_concurrent_requests)]
        # start_urls = [
        #     "https://ekantipur.com/koseli/2025/01/11/prithvi-narayan-shah-some-truth-some-delusion-04-06.html",
        #     "https://ekantipur.com/photo_feature/2025/01/11/president-places-wreath-at-prithvi-narayans-salik-photos-14-01.html"
        # ]
        
        print(f'\n\n start:{start_urls} \n\n')
        for url in start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.errback_httpbin)  # , dont_filter=True  : allows visiting same url again
    def parse(self, response):
        if response.status == 200:
            # 1) Save html content to a pickle file
            PickleUtils.save_html(response.url, response.body)

            # only send same domain links to mongo (if we ever need other links, we can get them from html content later)
            links = LinkExtractor(deny_extensions=[]).extract_links(response)
            

            # Next Page to Follow: 
            the_to_crawl_urls = []    # list for bulk upload
            for site_link in links:
                # print(f' \n muji site link: {site_link.url} \n')
                # base_url, crawl_it = should_we_crawl_it(site_link.url)  # self.visited_urls_base,
                de_fragmented_url = remove_fragments_from_url(site_link.url)
                if len(de_fragmented_url.strip()) > 0 and len(de_fragmented_url) < 250:
                    # avoid urls with length greater than 1000
                    the_to_crawl_urls.append(de_fragmented_url)
                    

            if the_to_crawl_urls:
                # self.mongo.append_url_to_crawl(the_to_crawl_urls)
                print('-'*50, f'to_crawl_urls:{the_to_crawl_urls}','-'*50, )
            
            ''' Note:
                If a url redirects to another url, then the original url is added to visited urls so as to not to visit it again.
                redirected url is sent via crawled_data and other_data then update visited_url.
            '''

            
            # Keep the worker buisy by getting new urls to crawl
            # Get few more start urls to crawl next
            number_of_new_urls_required = self.crawler.engine.settings.get('CONCURRENT_REQUESTS') - len(self.crawler.engine.slot.inprogress)
            if number_of_new_urls_required > 0:
                n_concurrent_requests = self.crawler.engine.settings.get('CONCURRENT_REQUESTS')
                fetched_start_urls = [remove_fragments_from_url(data_item['url']) for data_item in self.mongo.fetch_start_urls(number_of_new_urls_required)]
                if fetched_start_urls:
                    for url in fetched_start_urls:
                            # if url not in self.visited_urls:visited_urls.add(url)  # not necessary as we are fetching from mongo (filtered by server)
                            yield scrapy.Request(url, callback=self.parse)
            
        else:
            pass
            # save status to mongo
    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))
        response = failure.value.response
        
        if failure.check(HttpError):
            self.logger.error('HttpError on %s', response.url)

            error_data = {'url':response.url, 'timestamp':time.time(), 'status':'error', 'status_code':response.status, 'error_type':'HttpError'}
            self.mongo.append_error_data(error_data)
            # print(f'\n\n\n\n{error_data}\n\n\n\n\n\n')

            if response.status in [400, 401, 403, 404, 405, 408, 429, 500, 502, 503, 504]:
                self.logger.error('Got %s response. URL: %s', response.status, response.url)
                # handle the error here

        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)
            
            # Save error data on mongodb
            error_data = {'url':response.url, 'timestamp':time.time(), 'status':'error', 'status_code':response.status, 'error_type':'DNSLookupError'}
            self.mongo.append_error_data(error_data)
            # print(f'\n\n\n\n{error_data}\n\n\n\n\n\n')

        elif failure.check(TCPTimedOutError, TimeoutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

            # Save error data on mongodb
            error_data = {'url':response.url, 'timestamp':time.time(), 'status':'error', 'status_code':response.status, 'error_type':'TimeoutError'}
            self.mongo.append_error_data(error_data)
            # print(f'\n\n\n\n{error_data}\n\n\n\n\n\n')
        else:
            self.logger.error('Unknown error on %s', failure.request.url)

            # Save error data on mongodb
            error_data = {'url': failure.request.url, 'timestamp': time.time(), 'status': 'error', 'status_code': response.status, 'error_type': 'Unknown'}
            # print(f'\n\n oh oo.. Unknown error : {error_data}')
            self.mongo.append_error_data(error_data)