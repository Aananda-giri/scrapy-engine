from mongo import Mongo
mongo = Mongo()


import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

import os, time

def display_stats():
        to_crawl_count = mongo.collection.count_documents({"status": "to_crawl"})
        to_crawl_spider_count = mongo.collection.count_documents({"status": 'to_crawl?'})
        crawling_count = mongo.collection.count_documents({"status": "crawling"})
        crawled_count = mongo.collection.count_documents({"status": "crawled"})
        crawled_data_count = mongo.db['crawled_data'].count_documents({})
        other_data_count = mongo.db['other_data'].count_documents({})
        error_url_count = mongo.collection.count_documents({"status": "error"})
        # to_crawl_sqlite_count = URLDatabase(db_path="urls.db").count_entries("to_crawl")
        # crawled_sqlite_count = URLDatabase(db_path="urls.db").count_entries("crawled")

        # Nice formatted view for to_crawl, crawled and crawling
        # Formatted output
        print("=================================================")
        print("#  *********** Crawl Queue Status ***********")
        print("=================================================")
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}')
        print(f"\"to_crawl\" (Mongo): {locale.format_string('%d', to_crawl_count, grouping=True)}")
        print(f"\"to_crawl?\" (Mongo by Spider): {locale.format_string('%d', to_crawl_spider_count, grouping=True)}")
        print(f"Error (Mongo): {locale.format_string('%d', error_url_count, grouping=True)}")
        print(f"Crawling (Mongo): {locale.format_string('%d', crawling_count, grouping=True)}")
        print(f"Crawled_data (Mongo): {locale.format_string('%d', crawled_data_count, grouping=True)}")
        print(f"Other_data (Mongo): {locale.format_string('%d', other_data_count, grouping=True)}")
        # print(f"\"to_crawl\" (Sqlite): {locale.format_string('%d', to_crawl_sqlite_count, grouping=True)}")
        # print(f"\"crawled\" (Sqlite): {locale.format_string('%d', crawled_sqlite_count, grouping=True)}")
        # print(f"Crawled: {locale.format_string('%d', crawled_count, grouping=True)}")



        # Get mongo stats
        # stats = mongo.db.command("dbstats")
        # Print the stats
        # print("DB Size: ", stats['dataSize']/(1024*1024))
        # print("Storage Size: ", stats['storageSize']/(1024*1024))
        # print("Mongo Free Storage Space: ", stats['totalFreeStorageSize']/(1024*1024), end="\n-----------------------------------------------\n")

        # Crawling Rate
        # newly_crawled = crawled_count - number_of_links_crawled_at_start
        # time_taken = time.time() - start_time
        # crawling_rate = newly_crawled / time_taken
        # print(f"Crawling Rate: {locale.format_string('%d', crawling_rate, grouping=True)} links/sec")
        # expected_time_to_crawl = to_crawl_count / (crawling_rate if crawling_rate > 0 else 0.0000001)
        # print(f"Crawling Rate: {crawling_rate} links/sec")
        # print(f"Expected Time to Crawl: {locale.format_string('%d', expected_time_to_crawl/(60*60*24), grouping=True)} days")

        # Get Crawled File Size
        print(f"Size \"crawled_data.csv\": {os.path.getsize('crawled_data.csv')/(1024*1024) if os.path.exists('crawled_data.csv') else 0} MB")
        print(f"Size of \"other_data.csv\": {os.path.getsize('other_data.csv')/(1024*1024) if os.path.exists('other_data.csv') else 0} MB")
        print(f"Size of urls.db: {os.path.getsize('urls.db')/(1024*1024) if os.path.exists('urls.db') else 0} MB")
        print("===============================================")

if __name__ == '__main__':
        display_stats()
