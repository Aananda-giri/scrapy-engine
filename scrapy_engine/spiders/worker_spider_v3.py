
from .functions import is_social_media_link, is_document_link, is_google_drive_link, is_same_domain, is_np_domain,is_special_domain_to_crawl, load_env_var_in_google_colab, remove_fragments_from_url, is_nepali_language, is_valid_text_naive, is_document_or_media
# import pybloom_live
import scrapy
import dotenv
import json
# import os
# import redis
# import threading
import time

# from scrapy import signals# , Spider
from scrapy.linkextractors import LinkExtractor
from server.mongo import Mongo

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError, TimeoutError


class MasterSlave(scrapy.Spider):
    name = "worker_spider_v2"
    crawled_data = []
    to_visit = []
    other_data = []
    
            
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
        
        # default: Do not Crawl other data
        self.crawl_other_data = configs['crawl_other_data'] if 'crawl_other_data' in configs else False
        print(f'config.crawl_other_data: {self.crawl_other_data}')
        
        # default: Crawl paragraph data
        self.crawl_paragraph_data = configs['crawl_paragraph_data'] if 'crawl_paragraph_data' in configs else True
        print(f'config.crawl_paragraph_data: {self.crawl_paragraph_data}')
        print('------------------------------------')
        # ----------------------------------------------------------------------------

        # self.redis_client = redis.Redis(
        #     host=os.environ.get('REDIS_HOST', 'localhost'),
        #     port = int(os.environ.get('REDIS_PORT', 6379)),
        #     password=os.environ.get('REDIS_PASSWORD', None),
        # )
        
        # self.start_urls = self.fetch_start_urls()
        # self.start_urls = [data_item['url'] for data_item in self.mongo.fetch_start_urls()]
        # print(f'\n\n start_urls: {self.start_urls} \n\n')
        # for url in self.start_urls:
        #     yield scrapy.Request(url, callback=self.parse, errback=self.errback_httpbin)  # , dont_filter=True  : allows visiting same url again

        # using bloom filter to store visited urls for faster lookup
        # self.visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)    # pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH means
    
    # def fetch_start_urls(self, number_of_new_urls_required=10):
    #     return [json.loads(url) for url in self.redis_client.srandmember('urls_to_crawl_cleaned_set', number_of_new_urls_required)]
    def start_requests(self):
        n_concurrent_requests = self.crawler.engine.settings.get('CONCURRENT_REQUESTS')
        start_urls = [remove_fragments_from_url(data_item['url']) for data_item in self.mongo.fetch_start_urls(n_concurrent_requests)]
        print(f'\n\n start:{start_urls} \n\n')
        for url in start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.errback_httpbin)  # , dont_filter=True  : allows visiting same url again
    def parse(self, response):
        if 'text/html' not in response.headers.get('Content-Type', '').decode('utf-8'):
            # response is not text/html (its probably some document)
            # save the link to mongo
            self.mongo.db['other_data'].insert_one(
                {
                    'url': response.url,
                    'Content-Type': response.headers.get('Content-Type', '').decode('utf-8'),
                }
            )
            return
        links = LinkExtractor(deny_extensions=[]).extract_links(response)
        drive_links = []    # Drive links
        site_links = []     # website links
        
        # yield every heading, paragraph from current page
        headings_and_paragraphs = response.css('h1::text, h2::text, h3::text, h4::text, h5::text, h6::text, p::text').getall()
        if self.crawl_paragraph_data:
            # print('-------------------------------------------------')
            # print(f'\ncrawling paragraph_data: {self.crawl_paragraph_data}')
            # print('-------------------------------------------------')
            # Crawl Paragraph Data only if config is set to True
            the_crawled_data = {
                            'parent_url': response.url,
                            'page_title': response.css('title::text').get(),
                            'paragraphs': []  # list for bulk upload
                            # 'is_nepali_confidence': confidence
            }
            for paragraph in headings_and_paragraphs:
                is_nepali, confidence = is_nepali_language(paragraph)
                if is_nepali and is_valid_text_naive(paragraph):   # implement a way to detect nepali language (langid is not reliable)
                    # Save crawled data to redis
                    # self.crawled_data.append(
                    if paragraph not in the_crawled_data['paragraphs']:
                        # set would give better time complexity but they dont preserve order.
                        the_crawled_data['paragraphs'].append(paragraph)
                    # self.mongo.db['crawled_data'].insert_one(the_crawled_data)
                    '''
                    # Old Code using Redis: Redis turned out to be too slow for concurrent processes
                    self.redis_client.lpush('crawled_data', json.dumps(the_crawled_data))
                    '''
            
            if the_crawled_data:
                the_crawled_data['paragraphs'] = json.dumps(the_crawled_data['paragraphs'])
                try:
                    self.mongo.db['crawled_data'].insert_one(the_crawled_data)
                except DuplicateKeyError:
                    pass    # pass DuplicateKeyError. it means data already exists
                except Exception as Ex:
                    print(Ex)
                self.mongo.db['crawled_data'].insert(the_crawled_data)
        else:
            pass
            # print('-------------------------------------------------')
            # print(f'Avoided crawling paragraph_data: {not self.crawl_paragraph_data}')
            # print('-------------------------------------------------')
        # Saving drive links
        if self.crawl_other_data:
            # print('-------------------------------------------------')
            # print(f'\ncrawling_other_data: {self.crawl_other_data}')
            # print('-------------------------------------------------')
            # config['crawl_other_data'] is True
            # So crawl other data
            the_other_data =  []    # list for bulk upload
            for link in links:
                is_drive_link, _ = is_google_drive_link(link.url)     #DriveFunctions.is_google_drive_link(link.url)
                if is_drive_link:
                    the_other_data.append({
                            'parent_url': response.url,
                            'url': link.url,
                            'text': link.text,
                            'link_type': "drive_link",
                            'link_description': None
                    })
                    # self.mongo.db['other_data'].insert_one(the_other_data)
                    '''
                    # Old Code using Redis: Redis turned out to be too slow for concurrent processes
                    # # self.other_data.append({
                    self.redis_client.lpush('other_data', json.dumps(the_other_data))
                    '''
                else:
                    is_document, document_type, ignore_doc = is_document_link(link.url)
                    if is_document:
                        if not ignore_doc:  # and doc_type != 'image'
                            the_other_data.append({
                                'parent_url': response.url,
                                'url': link.url,
                                'text': link.text,
                                'link_type': "document",
                                'link_description': document_type
                            })
                            # self.mongo.db['other_data'].insert_one(the_other_data)

                            '''
                            # Old Code using Redis: Redis turned out to be too slow for concurrent processes
                            # self.other_data.append({
                            self.redis_client.lpush('other_data', json.dumps(the_other_data))
                            '''
                    else:
                        is_social_media, social_media_type = is_social_media_link(link.url)
                        if is_social_media:
                            the_other_data.append({
                                'parent_url': response.url,
                                'url': link.url,
                                'text': link.text,
                                'link_type': "social_media",
                                'link_description': social_media_type
                            })
                            # self.mongo.db['other_data'].insert_one(the_other_data)
                            '''
                            # Old Code using Redis: Redis turned out to be too slow for concurrent processes
                            self.redis_client.lpush('other_data', json.dumps())
                            '''
                        else:
                            site_links.append(link)
            if the_other_data: self.mongo.db['other_data'].insert_many(the_other_data)
        else:
            # print('-------------------------------------------------')
            # print(f'Avoided crawling_other_data: {not self.crawl_other_data}')
            # print('-------------------------------------------------')
            # config['crawl_other_data'] is False
            # So do not crawl other data
            for link in links:
                # To avoid crawling documents or social media links
                
                if not is_document_or_media(link.url):
                    site_links.append(link)

        # Next Page to Follow: 
        the_to_crawl_urls = []    # list for bulk upload
        for site_link in site_links:
            # print(f' \n muji site link: {site_link.url} \n')
            # base_url, crawl_it = should_we_crawl_it(site_link.url)  # self.visited_urls_base,
            de_fragmented_url = remove_fragments_from_url(site_link.url)
            if (is_np_domain(de_fragmented_url) or is_special_domain_to_crawl(de_fragmented_url)): # and de_fragmented_url not in self.visited_urls:
                # is_same_domain(response.url, de_fragmented_url) or    # it is a problem because attempting to crawl bbc.com/nepali can lead to bbc.com
                # only follow urls from same domain or other nepali domain and not visited yet
                # if len(self.crawler.engine.slot.inprogress) >= self.crawler.engine.settings.get('CONCURRENT_REQUESTS'):
                #     # send new urls_to_crawl to redis.
                #     # self.redis_client.sadd('url_to_crawl', json.dumps(de_fragmented_url))
                #     # self.mongo.append_url_to_crawl(de_fragmented_url)
                
                # self.mongo.append_url_to_crawl(de_fragmented_url)
                if len(de_fragmented_url.strip()) > 0 and len(de_fragmented_url) < 1000:
                    # avoid urls with length greater than 1000
                    the_to_crawl_urls.append(de_fragmented_url)
                
                # else:
                #     if self.mongo.append_url_crawling(de_fragmented_url):
                #         # yield url to crawl new urls_to_crawl by itself
                #         yield scrapy.Request(de_fragmented_url, callback=self.parse, errback=self.errback_httpbin)  # dont_filter=True  : allows visiting same url again
                    
                #         # self.to_visit.append(de_fragmented_url)
                        
                #         # Send crawling notice to server
                #         # self.redis_client.sadd('url_crawling', json.dumps(de_fragmented_url))
        if the_to_crawl_urls: self.mongo.append_url_to_crawl(the_to_crawl_urls)
        # append crawled url to visited urls
        # self.mongo.delete_to_crawl(response.request.url)
        ''' Note:
            If a url redirects to another url, then the original url is added to visited urls so as to not to visit it again.
            redirected url is sent via crawled_data and other_data then update visited_url.
        '''

        # Keep the worker buisy by getting new urls to crawl
        # Get few more start urls to crawl next
        number_of_new_urls_required = self.crawler.engine.settings.get('CONCURRENT_REQUESTS') - len(self.crawler.engine.slot.inprogress)
        if number_of_new_urls_required > 0:
            n_concurrent_requests = self.crawler.engine.settings.get('CONCURRENT_REQUESTS')
            fetched_start_urls = [remove_fragments_from_url(data_item['url']) for data_item in self.mongo.fetch_start_urls(n_concurrent_requests)]
            # fetched_start_urls = [remove_fragments_from_url(data_item['url']) for data_item in self.mongo.fetch_start_urls(number_of_new_urls_required)]
            if fetched_start_urls:
                for url in fetched_start_urls:
                        # if url not in self.visited_urls:  # not necessary as we are fetching from mongo
                        # self.visited_urls.add(url)
                        # print(url)
                        yield scrapy.Request(url, callback=self.parse)

    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        if failure.check(HttpError):
            response = failure.value.response
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