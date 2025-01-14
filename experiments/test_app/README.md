# Experiment1: relative links

```
scrapy shell "http://127.0.0.1:5000/"
from scrapy.linkextractors import LinkExtractor
links = LinkExtractor(deny_extensions=[]).extract_links(response)

# Gives full urls like:
'''
Link(url='http://otherdomain.com/page6', text='External Link (Should be ignored by Scrapy if configured)', fragment='', nofollow=False),
Link(url='http://127.0.0.1:5000/relative/path/page7', text='Relative Path', fragment='', nofollow=False),
Link(url='http://127.0.0.1:5000/footer_link', text='Footer Link', fragment='', nofollow=False)]
'''

import sys
sys.path.append('/mnt/resources2/weekly-projects/scrapy_engine_v2/scrapy_engine/scrapy_engine/spiders')
from urls_filter import WebPageURLFilter

filter = WebPageURLFilter()
[link for link in links if not filter.is_likely_webpage(link.url)]
# None

len([link for link in links if filter.is_likely_webpage(link.url)]) == len(links)
# True
```
