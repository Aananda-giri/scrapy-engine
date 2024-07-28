# Data types
local_mongo:

```
[{
    'url': entry['url'],
    'status': 'to_crawl',
    'timestamp': entry['timestamp']
},
{
    'url': 'existing_url_with_status_crawled',
    'status': 'crawled',
    'timestamp': time.time()
}]


* Status: <str>
    * to_crawl: url to crawl
    * recovered : url recovered from online_mongo whose status was `crwaling` for more than 5 minutes
    * crawling: url sent to online_mongo to crawl
    * crawled: url crawled
```