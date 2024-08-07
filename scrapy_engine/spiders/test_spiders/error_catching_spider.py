
from .functions import is_social_media_link, is_document_link, is_google_drive_link, is_same_domain, is_np_domain,is_special_domain_to_crawl, load_env_var_in_google_colab, remove_fragments_from_url, is_nepali_language, is_valid_text_naive, is_document_or_media
import scrapy
# import pybloom_live
import scrapy
import dotenv
import json
import os
# import redis
import threading
import time

from scrapy import signals# , Spider
from scrapy.linkextractors import LinkExtractor
# from server.mongo import Mongo

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError, TimeoutError


class MasterSlave(scrapy.Spider):
    name = "error_catching_spider"
    crawled_data = []
    to_visit = []
    other_data = []
    
            
    def __init__(self, *args, **kwargs):
        print(f'\n\n init...')


        
    def start_requests(self):
        start_urls = ['https://daodolpa.moha.gov.np/page/gallery'] # > (failed 3 times): 500 Internal Server Error
        start_urls.append('https://daorupandehi.moha.gov.np/%E0%A4%88%E0%A4%B2%E0%A4%BE%E0%A4%95%E0%A4%BE%20%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%B6%E0%A4%BE%E0%A4%B8%E0%A4%A8%20%E0%A4%95%E0%A4%BE%E0%A4%B0%E0%A5%8D%E0%A4%AF%E0%A4%BE%E0%A4%B2%E0%A4%AF%20%20%E0%A4%9A%E0%A4%82%E0%A4%96%E0%A5%87%E0%A4%B2%E0%A5%80%20%E0%A4%9C%E0%A4%BE%E0%A4%9C%E0%A4%B0%E0%A4%95%E0%A5%8B%E0%A4%9F') # 404
        start_urls.append('https://shivrajmun.gov.np/en/content/%E0%A4%AA%E0%A5%82%E0%A4%B0%E0%A5%8D%E0%A4%A3%E0%A4%AC%E0%A4%B9%E0%A4%BE%E0%A4%A6%E0%A5%81%E0%A4%B0-%E0%A4%B5%E0%A4%BF%E0%A4%B6%E0%A5%8D%E2%80%8D%E0%A4%B5%E0%A4%95%E0%A4%B0%E0%A5%8D%E0%A4%AE%E0%A4%BE-0') # 403
        for url in start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.errback_httpbin)  # , dont_filter=True  : allows visiting same url again
    def parse(self, response):
        print('should be parsing...')
        print(f'response link: {response.url}')

    def errback_httpbin(self, failure):
        print(f'\n\n oh oo.. Error Triggered...\n\n')
        # log all failures
        self.logger.error(repr(failure))

        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

            error_data = {'url':response.url, 'timestamp':time.time(), 'status':'error', 'status_code':response.status, 'type_status':type(response.status),'error_type':'HttpError'}
            print(f' oh oo.. Httperror error : {error_data} {dir(failure)}')
            # self.mongo.append_error_data(error_data)
            # print(f'\n\n\n\n{error_data}\n\n\n\n\n\n')

            if response.status in [400, 401, 403, 404, 405, 408, 429, 500, 502, 503, 504]:
                self.logger.error('Got %s response. URL: %s', response.status, response.url)
                # handle the error here

        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)
            
            # Save error data on mongodb
            error_data = {'url':response.url, 'timestamp':time.time(), 'status':'error', 'status_code':response.status, 'error_type':'DNSLookupError'}
            print(f' oh oo.. DNSLookupError error : {error_data}')
            # self.mongo.append_error_data(error_data)

        elif failure.check(TCPTimedOutError, TimeoutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

            # Save error data on mongodb
            error_data = {'url':response.url, 'timestamp':time.time(), 'status':'error', 'status_code':response.status, 'error_type':'TimeoutError'}
            print(f' oh oo.. TCPTimedOutError error : {error_data}')
            # print(f'\n\n\n\n{error_data}\n\n\n\n\n\n')
            # self.mongo.append_error_data(error_data)
        else:
            self.logger.error('Unknown error on %s', failure.request.url)

            # Save error data on mongodb
            error_data = {'url': failure.request.url, 'timestamp': time.time(), 'status': 'error', 'status_code': response.status, 'error_type': 'Unknown'}
            print(f'\n\n oh oo.. Unknown error : {error_data}')
            # self.mongo.append_error_data(error_data)