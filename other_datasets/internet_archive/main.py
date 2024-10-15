# !pip install wayback -q

from wayback import WaybackClient

client = WaybackClient()
results = client.search('nasa.gov') # generator
results # <generator object WaybackClient.search at 0x7b0968df2260>

record = next(results)
print(record)

record.timestamp

'''
CdxRecord(key='gov,nasa)/', timestamp=datetime.datetime(1996, 12, 31, 23, 58, 47, tzinfo=datetime.timezone.utc), url='http://www.nasa.gov/', mime_type='text/html', status_code=200, digest='MGIGF4GRGGF5GKV6VNCBAXOE3OR5BTZC', length=1811, raw_url='https://web.archive.org/web/19961231235847id_/http://www.nasa.gov/', view_url='https://web.archive.org/web/19961231235847/http://www.nasa.gov/')
datetime.datetime(1996, 12, 31, 23, 58, 47, tzinfo=datetime.timezone.utc)
'''

import requests
a=requests.get(record.view_url)
a.text  # html data







# -----------------------------------------
# how many times does word mars appears?
# -----------------------------------------
from wayback import Mode

# `Mode.original` is the default and doesn't need to be explicitly set;
# we've set it here to show how you might choose other modes.
response = client.get_memento(record, mode=Mode.original)

# this is  html content
content = response.content.decode()
content.count('mars')