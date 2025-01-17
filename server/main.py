from add_start_urls import add_start_urls
from crawled_data_server import run_crawled_data_server

import logging

from mongo import Mongo
import threading
from urls_server import run_url_server

from bloom import get_bloom_thread

mongo_db = Mongo()
logger = logging.getLogger(__name__)



# add start urls to to_crawl_urls
add_start_urls(include_our_unique=True, include_nepberta_unique=False, include_iriis_unique=False)


# run_url_server(mongo_db.collection, logger)

# run_crawled_data_server(mongo_db.collection, logger)


error_bloom = get_bloom_thread(save_file='error_bloom_filter.pkl', scalable=True)
to_crawl_bloom = get_bloom_thread(save_file='to_crawl_bloom_filter.pkl', scalable=True) 


url_server_thread = threading.Thread(target=run_crawled_data_server, args=(mongo_db.collection, logger), name='CrawledDataThread')
url_server_thread.daemon = True     # daemon threads are abruptly terminated when main program exits, even if they are still running
url_server_thread.start()

crawled_data_server_thread = threading.Thread(target=run_url_server, args=(mongo_db.collection, logger, error_bloom, to_crawl_bloom), name='URLServerThread')
crawled_data_server_thread.daemon = True    # daemon threads are abruptly terminated when main program exits, even if they are still running
crawled_data_server_thread.start()

url_server_thread.join()    # wait for url_server_thread to stopt (url_server_thread won't stop unless there is an error)
crawled_data_server_thread.join()