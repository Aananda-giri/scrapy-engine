from mongo import Mongo
from urls_server import run_url_server
from crawled_data_server import run_crawled_data_server

import threading

mongo_db = Mongo()
logger = logging.getLogger(__name__)



# run_url_server(mongo_db.collection, logger)

# run_crawled_data_server(mongo_db.collection, logger)

url_server_thread = threading.Thread(target=run_crawled_data_server, args=(mongo_db.collection, logger), name='CrawledDataThread')
crawled_data_thread.daemon = True
crawled_data_thread.start()

crawled_data_server_thread = threading.Thread(target=run_url_server, args=(mongo_db.collection, logger), name='URLServerThread')
url_server_thread.daemon = True
url_server_thread.start()