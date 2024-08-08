from .functions import is_social_media_link, is_nepali_language, is_document_link, is_google_drive_link, should_we_crawl_it, is_same_domain, is_np_domain, get_resume_urls

import scrapy
# from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import pybloom_live
# sys.path.append('/mnt/resources/programming/hackathons/pdf_engine/google_drive_api/python/crawler/ec2-upload/')
# from drive_functions import DriveFunctions

import json
def get_one_url():
    with open("urls_ekantipur.json",'r') as file:
        urls = json.load(file)
    
    one_url = urls.pop()
    
    with open("urls_ekantipur.json",'w') as file:
        urls = json.dump(urls, file)
    
    return one_url

class MySpider(scrapy.Spider):
    '''
    crawl all sub-domains of domain given in start_urls
    crawl first 1000 '.np' domains and their sub-domains
    '''
    name = 'ekantipur'
    # List of starting URLs
    # reference: https://www.w3newspapers.com/nepal/, https://onlinenewspapers.com/nepal.shtml
    
    nepali_newspapers = ['https://onlinetvnepal.com/', 'https://radiosagarmatha.org.np/', 'https://www.onlinekhabar.com/', 'https://samacharpati.com/', 'https://aarthiknews.com/', 'https://sancharkendra.com/', 'https://www.eadarsha.com/', 'https://www.bizshala.com/', 'https://abhiyandaily.com/', 'https://bigulnews.com/', 'https://kantipath.com/', 'https://www.nayapage.com/', 'https://gorkhapatraonline.com/', 'http://www.saharasansar.com/', 'https://nagariknews.nagariknetwork.com/', 'https://nepalsamaya.com/', 'https://www.janaaastha.com/', 'https://www.nepalipatra.com/', 'https://www.khojtalashonline.com/', 'https://bizkhabar.com/', 'https://www.nayapatrikadaily.com/', 'https://hellokhabar.com/', 'https://www.karobardaily.com/', 'https://www.setopati.com/', 'https://www.souryaonline.com/', 'https://www.nagariknetwork.com/', 'https://www.khasokhas.com/', 'https://rajdhanidaily.com/', 'https://www.ratopati.com/', 'https://bigulnews.tv/', 'https://lokaantar.com/', 'https://thahakhabar.com/', 'https://www.himalkhabar.com/', 'https://radionepal.gov.np/', 'https://www.osnepal.com/', 'https://www.arghakhanchi.com/', 'https://realkhabar.net/', 'https://baahrakhari.com/', 'https://www.news24nepal.tv/', 'https://www.hamrokhelkud.com/', 'https://hamrakura.com/', 'https://www.onsnews.com/', 'https://annapurnapost.com/', 'https://www.koshionline.com/', 'https://samudrapari.com/', 'https://arthasarokar.com/', 'https://www.newsofnepal.com/', 'https://ujyaaloonline.com/', 'https://onlinemajdoor.com/', 'http://nepalipost.com/beta/', 'https://nepalkhabar.com/', 'https://nepalihimal.com/', 'https://hamrokhotang.com/', 'https://www.nepalipaisa.com/', 'https://topnepalnews.com/', 'https://www.bbc.com/nepali', 'https://saptahik.com.np/'] # , 'https://ekantipur.com/'
    hindi_newspapers = ['https://www.himalini.com/']
    english_newspapers = ['https://thehimalayantimes.com/', 'https://risingnepaldaily.com/', 'https://www.goalnepal.com/', 'https://nepaliheadlines.com/', 'https://www.newbusinessage.com/', 'https://kathmandutribune.com/', 'https://kathmandupost.com/', 'https://nepalitimes.com/', 'https://www.peoplesreview.com.np/']
    error_newspapers = ["http://epaperhimalayadarpan.com/", "https://ejanakpurtoday.com/", "https://glocalkhabar.com/", "https://www.chitawan.com/", "https://www.harekpalnews.com/", "https://ujyaaloonline.com/"]
    nepberta_urls = ["https://ekantipur.com/",  "https://onlinekhabar.com/",  "https://nagariknews.com/",  "https://thahakhabar.com/",  "https://ratopati.com/",  "https://reportersnepal.com/",  "https://setopati.com/",  "https://hamrakura.com/",  "https://lokpath.com/",  "https://abhiyandaily.com/",  "https://pahilopost.com/",  "https://lokaantar.com/",  "https://dcnepal.com/",  "https://nayapage.com/",  "https://nayapatrikadaily.com/",  "https://everestdainik.com/",  "https://imagekhabar.com/",  "https://shilapatra.com/",  "https://khabarhub.com/",  "https://baahrakhari.com/",  "https://ujyaaloonline.com/",  "https://nepalkhabar.com/",  "https://emountaintv.com/",  "https://kathmandupress.com/",  "https://farakdhar.com/",  "https://kendrabindu.com/",  "https://dhangadhikhabar.com/",  "https://gorkhapatraonline.com/",  "https://nepalpress.com/",  "https://hamrokhelkud.com/",  "https://himalkhabar.com/",  "https://nepallive.com/",  "https://nepalsamaya.com/",  "https://kalakarmi.com/",  "https://dainiknewsnepal.com/"]
    
    # start_urls = get_one_url()
    
    def __init__(self, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        
        self.start_urls = [kwargs.get('start_url')] 
        # using bloom filter to store visited urls for faster lookup
        self.visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)    # pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH means
        domain_name_to_resume_from = kwargs.get('domain_name_to_resume_from')
        if domain_name_to_resume_from:
            resume_urls, visited_urls = get_resume_urls(domain_name_to_resume_from)
            if resume_urls:
                self.start_urls = resume_urls
                self.visited_urls = visited_urls
                print(f'\n\n Loaded:{len(self.start_urls)} urls.\n\n')
            
        print(f'\n-----------------------------\n start_urls: {self.start_urls if len(self.start_urls)<=1 else len(self.start_urls)} domain_name_to_resume_from : {domain_name_to_resume_from}\n-----------------------------\n')
        
        # self.visited_urls_base = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH)
        # self.length = 0
    
    def parse(self, response):
        from scrapy.linkextractors import LinkExtractor
        links = LinkExtractor(deny_extensions=[]).extract_links(response)
        drive_links = []    # Drive links
        site_links = []     # website links
        
        yield {
            'visited': response.url,
            }

        # yield every paragraph from current page
        paragraphs = response.css('p::text').getall()
        for paragraph in paragraphs:
            is_nepali, confidence = is_nepali_language(paragraph)
            if is_nepali:
                # print(paragraph)
                yield {
                        'parent_url': response.url,
                        'page_title': response.css('title::text').get(),
                        'paragraph': paragraph,
                        'is_nepali_confidence': confidence
                    }

        # Saving drive links
        for link in links:
            is_drive_link, _ = is_google_drive_link(link.url)     #DriveFunctions.is_google_drive_link(link.url)
            if is_drive_link:
                yield {
                    'parent_url': response.url,
                    'url': link.url,
                    'text': link.text,
                    'is_drive_link': True
                }
            else:
                is_document, document_type, ignore_doc = is_document_link(link.url)
                if is_document:
                    if not ignore_doc:  # and doc_type != 'image'
                        yield {
                            'parent_url': response.url,
                            'url': link.url,
                            'text': link.text,
                            'is_document': is_document,
                            'document_type': document_type

                        }
                else:
                    is_social_media, social_media_type = is_social_media_link(link.url)
                    if is_social_media:
                        yield {
                            'parent_url': response.url,
                            'url': link.url,
                            'text': link.text,
                            'is_social_media': is_social_media,
                            'social_media_type': social_media_type

                        }
                    else:
                        site_links.append(link)
        
        # Follow Next Page: 
        for site_link in site_links:
            
            # base_url, crawl_it = should_we_crawl_it(site_link.url)  # self.visited_urls_base,
            # self.visited_urls_base.add(base_url)
            
            
                
            if is_same_domain(response.url, site_link.url): # or crawl_it:
                # only follow urls from same domain
                
                # check if url not already visited
                if site_link.url not in self.visited_urls:
                        # if self.length<10:
                        self.visited_urls.add(site_link.url)
                        yield scrapy.Request(site_link.url, callback=self.parse)
                        
                        yield {
                            'to_visit': site_link.url
                            }
            
            elif is_np_domain(site_link.url):
                yield {
                        'parent_url': response.url,
                        'url': site_link.url,
                        'text': site_link.text,
                        'np_domain': True,
                    }
    def save_error_url(self, url, error):
        # Load existing error URLs or create an empty list
        try:
            with open('error_urls.json', 'r') as f:
                error_urls = json.load(f)
        except FileNotFoundError:
            error_urls = []

        # Add the current URL to the list
        error_urls.append({
            "url":url, 
            "error": error
        })

        # Save the updated list back to error_urls.json
        with open('error_urls.json', 'w') as f:
            json.dump(error_urls, f, indent=2)

        self.log(f"URL added to error_urls.json: {url}")

