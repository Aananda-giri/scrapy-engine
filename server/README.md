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
```