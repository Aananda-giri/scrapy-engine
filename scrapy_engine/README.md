[] post-process: if crawled data is str: make it list
[] upload code to zyte
[X] drive rename spider to worker_spider

# To run the code
`scrapy crawl worker_spider -o worker_spider.json`

# TODO
*[ ] https://cmc.edu.np/hospital/doctors/67
        /worker_spider_v3.py", line 295, in errback_httpbin
        error_data = {'url': failure.request.url, 'timestamp': time.time(), 'status': 'error', 'status_code': response.status, 'error_type': 'Unknown'}
        AttributeError: 'NoneType' object has no attribute 'status'
* [x] crawled_data
        [X]current_headers: ['parent_url', 'page_title', 'paragraph']
        [x]new_headers: ['parent_url', 'page_title', 'paragraphs']

                where, paragraphs is list of paragraph from current_headers
        [X] upload to drive and delete `crawled_data.csv` (new data with column `paragraphs` instead of `paragraph` will be added)
        [X] update zyte code (maybe push via github)
* [ ] zyte code auto push on github update
* [ ] load bloom function from oscar dataset to avoid crawling urls crawled by oscar again.
* [ ] Auto generate zyte version of code
* [ ] Handle pdf urls as in pdf_engine
* [ ] 2024-08-08 08:29:25 [scrapy.core.scraper] ERROR: Spider error processing <GET https://nepmed.nhrc.gov.npindex.php/jnhrc/article/download/756/603/> (referer: None)
Traceback (most recent call last):
  File "/usr/local/lib/python3.10/dist-packages/twisted/internet/defer.py", line 892, in _runCallbacks
    current.result = callback(  # type: ignore[misc]
  File "/content/scrapy-engine/scrapy_engine/scrapy_engine/spiders/worker_spider_v3.py", line 258, in errback_httpbin
    response = failure.value.response
AttributeError: 'DNSLookupError' object has no attribute 'response'

* [ ] 2024-08-02 01:39:31 [scrapy.core.scraper] ERROR: Spider error processing <GET https://www.nrb.org.np/contents/uploads/2022/03/SP-4-Text-for-website.pdf> (referer: None)
File "/content/scrapy-engine/scrapy_engine/spiders/worker_spider_v2.py", line 77, in parse
AttributeError: Response content isn't text

* [ ] Set envs for zyte and run spider from github code


* [X] Error not handled
2024-07-31 03:43:33 [scrapy.downloadermiddlewares.retry] ERROR: Gave up retrying <GET https://daodolpa.moha.gov.np/page/gallery> (failed 3 times): 500 Internal Server Error
2024-07-31 03:43:34 [scrapy.spidermiddlewares.httperror] INFO: Ignoring response <500 https://daodolpa.moha.gov.np/page/gallery>: HTTP status code is not handled or not allowed
2024-07-29 04:20:28 [scrapy.spidermiddlewares.httperror] INFO: Ignoring response <404 https://daorupandehi.moha.gov.np/%E0%A4%88%E0%A4%B2%E0%A4%BE%E0%A4%95%E0%A4%BE%20%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%B6%E0%A4%BE%E0%A4%B8%E0%A4%A8%20%E0%A4%95%E0%A4%BE%E0%A4%B0%E0%A5%8D%E0%A4%AF%E0%A4%BE%E0%A4%B2%E0%A4%AF%20%20%E0%A4%9A%E0%A4%82%E0%A4%96%E0%A5%87%E0%A4%B2%E0%A5%80%20%E0%A4%9C%E0%A4%BE%E0%A4%9C%E0%A4%B0%E0%A4%95%E0%A5%8B%E0%A4%9F>: HTTP status code is not handled or not allowed
2024-07-29 04:42:14 [scrapy.spidermiddlewares.httperror] INFO: Ignoring response <404 https://churemun.gov.np/node/383>: HTTP status code is not handled or not allowed
2024-07-29 04:46:57 [scrapy.spidermiddlewares.httperror] INFO: Ignoring response <403 https://shivrajmun.gov.np/en/content/%E0%A4%AA%E0%A5%82%E0%A4%B0%E0%A5%8D%E0%A4%A3%E0%A4%AC%E0%A4%B9%E0%A4%BE%E0%A4%A6%E0%A5%81%E0%A4%B0-%E0%A4%B5%E0%A4%BF%E0%A4%B6%E0%A5%8D%E2%80%8D%E0%A4%B5%E0%A4%95%E0%A4%B0%E0%A5%8D%E0%A4%AE%E0%A4%BE-0>: HTTP status code is not handled or not allowed


