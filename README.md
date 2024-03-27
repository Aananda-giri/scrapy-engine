## Test run the scrapy
`scrapy crawl quotes -O quotes.json`
`scrapy crawl ekantipur_v4 -O hamrakura.json -a start_url="https://www.hamrakura.com/"`

newly assigned digital ocean:
["https://www.newsofnepal.com/",  "https://ujyaaloonline.com/"

previously assigned:
["http://nepalipost.com/beta/", "https://www.nepalipaisa.com/", "https://topnepalnews.com/", "https://www.dainiknepal.com/", "httpsf://nepalkhabar.com/"]

# Simple Scrapy scripts to crawl news

# To run the script
python3 main.py



ls -l --block-size=M



# load csv data
```
rows=[]
with open('nepali_dataset.csv', 'r') as csvfile:
  # Create a reader object
  csv_reader = csv.reader(csvfile)
  # Read the data row by row
  for row in csv_reader:
      # Do something with the data in each row
      rows.append(row)q
```

# Urls to crawl from
```
nepberta_urls = ["https://ekantipur.com/",  "https://onlinekhabar.com/",  "https://nagariknews.com/",  "https://thahakhabar.com/",  "https://ratopati.com/",  "https://reportersnepal.com/",  "https://setopati.com/",  "https://hamrakura.com/",  "https://lokpath.com/",  "https://abhiyandaily.com/",  "https://pahilopost.com/",  "https://lokaantar.com/",  "https://dcnepal.com/",  "https://nayapage.com/",  "https://nayapatrikadaily.com/",  "https://everestdainik.com/",  "https://imagekhabar.com/",  "https://shilapatra.com/",  "https://khabarhub.com/",  "https://baahrakhari.com/",  "https://ujyaaloonline.com/",  "https://nepalkhabar.com/",  "https://emountaintv.com/",  "https://kathmandupress.com/",  "https://farakdhar.com/",  "https://kendrabindu.com/",  "https://dhangadhikhabar.com/",  "https://gorkhapatraonline.com/",  "https://nepalpress.com/",  "https://hamrokhelkud.com/",  "https://himalkhabar.com/",  "https://nepallive.com/",  "https://nepalsamaya.com/",  "https://kalakarmi.com/",  "https://dainiknewsnepal.com/"]


'''
* 'dainikonline' seems to have been closed or moved somewhere else.
* latest tweets in their twitter page are from 2019. https://twitter.com/dainikonline2?lang=en
'''
nepberta_error_urls  = ["https://dainikonline.com/"]    
new_urls = []

crawling_completed = ['https://www.hamrakura.com/',  'https://www.bigulnews.com/', 'https://www.baahrakhari.com/',  'https://www.ekantipur.com/',  'https://sancharkendra.com/',  'https://www.realkhabar.net/',  'https://www.nagariknetwork.com/',  'https://www.abhiyandaily.com/',  'https://www.eadarsha.com/',  'https://samacharpati.com/',  'https://www.hamrokhotang.com/',  'https://www.aarthiknews.com_merged_/',  'https://www.kantipath.com/',  'https://www.onlinetvnepal.com/',  'https://www.eadarshhistora.com_merged_/',  'https://www.arghakhanchi.com/',  'https://www.nayapage.com/',  'https://www.radiosagarmatha.org.np/',  'https://www.onsnews.com/',  'https://www.onlinekhabar.com_merged_/',  'https://www.saptahik.com.np/',  'https://www.sancharkendra.com_merged_/',  'https://www.samacharpati.com_merged_/',  'https://www.nepalihimal.com/',  'https://aarthiknews.com/']


have_crawled_incomplete = ["https://hamrakura.com/", "https://onlinekhabar.com/"]


need_special_attention = ["https://www.bbc.com/nepali", "https://beta.gorkhapatraonline.com/epapermaincategory", "https://epaper.gorkhapatraonline.com/", "https://ujyaaloonline.com/" -> 403 response by cloudfare]
'''
bbc_nepali: avoid following links that does not start with: bbc.com/nepali
gorkhapatra: it would be nice to crawl pdfs of gorkhapatra
            problem is: I coudn't find their encoding scheme
'''


```

# Post-Process
* `python3 post_process.py`
* to extract nepali paragraphs in csv format from crawled `.json` data.

```
from scrapy_engine.spiders.functions import merge_same_named_json_files
merge_same_named_json_files(delete_merged=True)
```


# How to know if crawling is completed for urls?
* command: `pytho3 resume_incomplete_crawling.py`
* get resume_urls from crawled data if any
* and resume crawling

# Data:
* [data-crawled-so-far](https://drive.google.com/drive/folders/1v_dv0H56D3J-56VDPIaBkJ601djs8C0w?usp=sharing)
