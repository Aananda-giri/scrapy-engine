from .functions import  load_env_var_in_google_colab, remove_fragments_from_url, is_same_domain
from .urls_filter import WebPageURLFilter

import scrapy
import dotenv
import json

# import redis
# import threading
import time

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError, TimeoutError, ConnectionRefusedError

import sys

from .pickle_utils import PickleUtils
sys.path.append('../server/../')
from .mongo import Mongo


from bs4 import BeautifulSoup
# from scrapy import signals# , Spider
from scrapy.linkextractors import LinkExtractor
from dataclasses import dataclass

@dataclass
class Link:
    url: str



class WorkerSpider(scrapy.Spider):
    name = "worker_spider"
    # crawled_data = []
    # to_visit = []
    # other_data = []
    
            
    def __init__(self, *args, **kwargs):
        # load environment variables if running in google colab
        load_env_var_in_google_colab()
        # dotenv.load_dotenv("server/.env")
        
        # to filter urls that are not likely web pages
        self.webpage_filter = WebPageURLFilter()

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
        
        # Comment to debug
        start_urls = [remove_fragments_from_url(data_item['url']) for data_item in self.mongo.fetch_start_urls(n_concurrent_requests)]
        
        # uncomment to debug
        # start_urls = [
            # 'https://www.ukaalo.com/',
        #     "https://ekantipur.com/koseli/2025/01/11/prithvi-narayan-shah-some-truth-some-delusion-04-06.html",
        #     "https://ekantipur.com/photo_feature/2025/01/11/president-places-wreath-at-prithvi-narayans-salik-photos-14-01.html"
        # ]
        
        print(f'\n\n start:{start_urls} \n\n')
        start_urls = [remove_fragments_from_url(data_item['url']) for data_item in self.mongo.fetch_start_urls(n_concurrent_requests)]
        for url in start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.errback_httpbin)  # , dont_filter=True  : allows visiting same url again
    def parse(self, response):
        '''
        * `response.body` is in binary format and
        * `response.text` is in (human readable) text based format.
        '''

        if response.status == 200 and 'text/html' in response.headers.get('Content-Type', '').decode('utf-8'):
            # 1) get redirect_links

            # only send same domain links to mongo (if we ever need other links, we can get them from html content later)
            
            
            soup_links = []
            links = LinkExtractor(deny_extensions=[]).extract_links(response)
            if not links:
                '''
                    linkextractors is giving 0 links for sites like:`http://deshsanchar.com`
                    so combining linkextractor with bss4
                    from scrapy.linkextractors import LinkExtractor
                    links = LinkExtractor(deny_extensions=[]).extract_links(response)
                    print(len(links))
                '''
                # Extract links using BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                soup_links = [Link(link['href']) for link in soup.find_all('a', href=True)]
                links = soup_links



            # Next Page to Follow: 
            redirect_links = []    # list for bulk upload
            for site_link in links:
                # print(f' \n muji site link: {site_link.url} \n')
                # base_url, crawl_it = should_we_crawl_it(site_link.url)  # self.visited_urls_base,
                de_fragmented_url = remove_fragments_from_url(site_link.url)
                                
                is_likely_a_webpage = self.webpage_filter.is_likely_webpage(site_link.url)
                if  is_likely_a_webpage and \
                    len(de_fragmented_url.strip()) > 0 and \
                    len(de_fragmented_url) < 250 and \
                    is_same_domain(response.url, de_fragmented_url):
                        # avoid urls with length greater than 250
                        redirect_links.append(de_fragmented_url)
            
            # uncomment to debug    
            # print(f'redirect_links:{redirect_links}')
            
            # limit html size
            MAX_HTML_SIZE = 7 * 1024 * 1024  # 7 MB
            
            # uncomment to debug
            print(f'url:{response.url}\n redirect_links: {len(redirect_links)} \n valid-content-len.{len(response.text) < MAX_HTML_SIZE}')
            # print(f'\n\n response.url:{response.url}')

            # Comment to debug
            if len(response.text) > MAX_HTML_SIZE:
                # Optionally log or handle oversized responses
                self.logger.warning(f"Skipped {response.url} as it exceeds the maximum size of {MAX_HTML_SIZE} bytes.")
            else:
                #  ignore content that exceeds MAX_HTML_SIZE rather than truncating it to preserve the HTML Integrity 
                # 1) Save html content to a pickle file
                PickleUtils.save_html(response_url = response.request.url, request_url = response.url, response_body = response.text, redirect_links = redirect_links)

            # if redirect_links:
            #     # self.mongo.append_url_to_crawl(redirect_links)
            #     print('-'*50, f'to_crawl_urls:{redirect_links}','-'*50, )
            
            ''' Note:
                If a url redirects to another url, then the original url is added to visited urls so as to not to visit it again.
                redirected url is sent via crawled_data and other_data then update visited_url.
            '''

            
            
            # Keep the worker buisy by getting new urls to crawl
            # Get few more start urls to crawl next
            # Comment to debug
            number_of_new_urls_required = self.crawler.engine.settings.get('CONCURRENT_REQUESTS') - len(self.crawler.engine.slot.inprogress)
            if number_of_new_urls_required > 0:
                n_concurrent_requests = self.crawler.engine.settings.get('CONCURRENT_REQUESTS')
                fetched_start_urls = [remove_fragments_from_url(data_item['url']) for data_item in self.mongo.fetch_start_urls(number_of_new_urls_required)]
                if fetched_start_urls:
                    for url in fetched_start_urls:
                            # if url not in self.visited_urls:visited_urls.add(url)  # not necessary as we are fetching from mongo (filtered by server)
                            yield scrapy.Request(url, callback=self.parse, errback=self.errback_httpbin)
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
        elif failure.check(ConnectionRefusedError):
            request = failure.request
            self.logger.error('ConnectionRefusedError on %s', request.url)

            error_data = {
                'url': request.url,
                'timestamp': time.time(),
                'status': 'error',
                'status_code': None,  # No status code for connection refused
                'error_type': 'ConnectionRefusedError'
            }
            self.mongo.append_error_data(error_data)
        else:
            self.logger.error('Unknown error on %s', failure.request.url)

            # Save error data on mongodb
            error_data = {'url': failure.request.url, 'timestamp': time.time(), 'status': 'error', 'status_code': response.status, 'error_type': 'Unknown'}
            # print(f'\n\n oh oo.. Unknown error : {error_data}')
            self.mongo.append_error_data(error_data)