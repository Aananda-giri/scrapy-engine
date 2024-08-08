import csv
import dotenv
import json
import os
import redis
import threading
import time
import pybloom_live
dotenv.load_dotenv()

redis_client = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port = int(os.environ.get('REDIS_PORT', 6379)),
    password=os.environ.get('REDIS_PASSWORD', None),
    )


def save_to_csv(data, filename='crawled_data.csv'):
    if type(data) == dict and 'paragraph' in data.keys():
        with open(filename, 'a') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            writer.writerow(data)
    else:
        # save to error_logs.json
        error_file_name = 'error_logs.json'
        if os.path.exists(error_file_name):
            with open(error_file_name,'r') as f:
                error_data = json.load(f)
            
            error_data.append(data)
            with open(error_file_name,'w') as f:
                json.dump(error_data, f)
            print(error_data)
        else:
            with open(error_file_name,'w') as f:
                json.dump([], f)

# for i in range(10):
#     save_to_csv({'paragraph': 'प्रदेश सरकार र निजी क्षेत्रको सहकार्यमा पहाडी तथा हिमाली क्षेत्रमा मनोरञ्जनात्मक तथा साहसिक पर्यटनको लागि हिलस्टेशनहरू विकास गर्न आवश्यक छ भन्दै उनले सांस्कृतिक, धार्मिक, साहसिक, कृषि, स्वास्थ्य तथा खेल पर्यटक आकर्षित गर्दै यस क्षेत्रको मौलिक संस्कृति संरक्षणमा महोत्सवले सहयोग गर्ने विश्वास व्यक्त गरे ।\xa0',
#     'parent_url': 'https://hamrakura.com/news-details/161504/2023-12-27',
#     'page_title': 'लोकतन्त्रको आन्दोलन उठाउँदाकै आस्थाबाट निर्देशित छु – राष्ट्रपति पौडेल',
#     'is_nepali_confidence': '-1914.427728056908'})

# save_to_csv({'paragraph': 'त्रिवेणी बाहेक अन्य पालिकाबाट कुन-कुन घर परिवारले रकम पाउने भन्ने विवरण नआइसकेकाले ती पालिकाका लागि रकम निकासा भने हुन सकेको छैन ।', 'parent_url': 'https://hamrakura.com/news-details/159820/2023-11-28', 'page_title': 'भूकम्पपीडितको अस्थायी आवासका लागि रकम निकासा', 'is_nepali_confidence':'-800.0689898729324'},)

def crawled_data():
    visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)    # pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH means
    data_count = 0
    while True:
        # print('while')
        data = redis_client.rpop('crawled_data')
        # print(f'got data')
        if data:
            data = json.loads(data)
            print(f'data paye:\n\t paragraph[:10]: {paragraph[:10]} \t keys: {data.keys()} ')
            print(f'{data["paragraph"][:5]}')
            if data['parent_url'] not in visited_urls:
                visited_urls.add(data['parent_url'])        # visited_urls: Saved locally using pybloom live
                # remove url if present in to_visit   # to_visit: Saved in redis
                save_to_csv(data)           # saved locally
                # print('saved csv')
                data_count += 1
                print(data_count)
        else:
            print("Data chaina. 10 sec. sutxu")
            time.sleep(10)
        # break
        # sleep(n_seconds)

def other_data():
    visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)    # pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH means
    data_count = 0
    while True:
        # print('while')
        data = redis_client.rpop('crawled_data')
        # print(f'got data')
        if data:
            data = json.loads(data)
            print(f'data paye:\n\t paragraph[:10]: {paragraph[:10]} \t keys: {data.keys()} ')
            print(f'{data["paragraph"][:5]}')
            if data['parent_url'] not in visited_urls:
                visited_urls.add(data['parent_url'])        # visited_urls: Saved locally using pybloom live
                # remove url if present in to_visit   # to_visit: Saved in redis
                save_to_csv(data)           # saved locally
                # print('saved csv')
                data_count += 1
                print(data_count)
        else:
            print("Data chaina. 10 sec. sutxu")
            time.sleep(10)
        # break
        # sleep(n_seconds)

def client_hardware():
    visited_urls = pybloom_live.ScalableBloomFilter(mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)    # pybloom_live.ScalableBloomFilter.SMALL_SET_GROWTH means
    data_count = 0
    while True:
        # print('while')
        data = redis_client.rpop('crawled_data')
        # print(f'got data')
        if data:
            data = json.loads(data)
            print(f'data paye:\n\t paragraph[:10]: {paragraph[:10]} \t keys: {data.keys()} ')
            print(f'{data["paragraph"][:5]}')
            if data['parent_url'] not in visited_urls:
                visited_urls.add(data['parent_url'])        # visited_urls: Saved locally using pybloom live
                # remove url if present in to_visit   # to_visit: Saved in redis
                save_to_csv(data)           # saved locally
                # print('saved csv')
                data_count += 1
                print(data_count)
        else:
            print("Data chaina. 10 sec. sutxu")
            time.sleep(10)
        # break
        # sleep(n_seconds)