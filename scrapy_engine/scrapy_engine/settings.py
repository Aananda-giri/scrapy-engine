# ==================================================================================
#    ////////////////////////////////////////////////////////////////////////////
# ==================================================================================

### **Recommended Scrapy Settings**
# Ubuntu machine 
# (1 vCPU, 512 MB RAM, and 10 GB SSD)

# ==================================================================================
#    ////////////////////////////////////////////////////////////////////////////
# ==================================================================================

#### **1. General Spider Settings**

CONCURRENT_REQUESTS = 4  # Keep concurrency low to reduce CPU and memory usage
DOWNLOAD_DELAY = 1       # Introduce a delay between requests to prevent overloading
CONCURRENT_REQUESTS_PER_DOMAIN = 2  # Limit requests per domain
CONCURRENT_REQUESTS_PER_IP = 2      # Limit requests per IP


'''#### **2. AutoThrottle**
Enable **AutoThrottle** to dynamically adjust request rates based on server responses. This helps reduce the load on both your machine and the target server.'''

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 2  # Initial delay
AUTOTHROTTLE_MAX_DELAY = 10   # Maximum delay in case of server overload
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0  # Requests sent in parallel (average)
AUTOTHROTTLE_DEBUG = False  # Set to True to see throttling stats in logs


'''#### **3. Memory Management**
Limit memory usage to prevent your spider from crashing due to low RAM.'''

MEMUSAGE_LIMIT_MB = 128  # Limit Scrapy's memory usage to 128 MB
MEMUSAGE_NOTIFY_MAIL = ['your-email@example.com']  # Notify if memory exceeds limit

'''#### **4. HTTP Caching**
Enable HTTP caching for development and testing. This reduces the number of requests to the server and improves performance.'''

HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600  # Cache for 1 hour
HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = [500, 503, 504, 400, 403, 404]  # Ignore these errors
HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'


'''#### **5. Downloader Middleware**
Minimize middleware usage to save memory. Only include essential middlewares.'''

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 400,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 500,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
}

'''#### **6. Logging**
Reduce logging verbosity to save disk space and improve performance.'''

LOG_LEVEL = 'INFO'  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT = '%(levelname)s: %(message)s'
LOG_FILE = 'scrapy.log'  # Save logs to a file

'''#### **7. Storage**
Since your SSD is limited to 10 GB, be mindful of storage when saving data.

- Use efficient formats like **Parquet** for structured data.
- Compress logs or enable rotation.

```
# for outputting scraped data
FEED_FORMAT = 'parquet'  # Efficient format
FEED_URI = 'data.parquet'
```
'''

'''#### **8. Request Fingerprinting**
Ensure compatibility with Scrapy's default request fingerprinting.'''

REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'


'''#### **9. Robot.txt**
By default, Scrapy respects `robots.txt`. Disable it only if necessary.
'''
ROBOTSTXT_OBEY = False  # Set to False only if you have permission to scrape

'''#### **10. Extensions**
Disable non-essential extensions to save resources.'''

EXTENSIONS = {
    'scrapy.extensions.corestats.CoreStats': 500,
    'scrapy.extensions.logstats.LogStats': 500,
    # Disable Telnet to save memory
    'scrapy.extensions.telnet.TelnetConsole': None,
}
# ==================================================================================
#    ////////////////////////////////////////////////////////////////////////////
# ==================================================================================

# Scrapy settings for scrapy_engine project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "scrapy_engine"

SPIDER_MODULES = ["scrapy_engine.spiders"]
NEWSPIDER_MODULE = "scrapy_engine.spiders"


# ============================================
# To upload crawled data to hf or s3
EXTENSIONS = {
    'scrapy_engine.extensions.BackgroundUploadExtension': 100,  # number determines load order
}


# Optional settings for the upload service
PICKLE_DIR = './pickles'
UPLOAD_SIZE_THRESHOLD_MB = 30.0
UPLOAD_INTERVAL_SECONDS = 10 # 10 # 1hr is equivaline to 3600 seconds
CHECK_INTERVAL_SECONDS = 2 # 2    # 5 minutes
# ============================================


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "scrapy_engine (+http://www.yourdomain.com)"

'''
# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS =  15 # 10 for replit # 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 1 # 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

'''
# Random header middleware
DOWNLOADER_MIDDLEWARES = {
    'scrapy_engine.middlewares.RandomHeaderMiddleware': 543,  # Adjust the priority as needed (lower number means higher priority)
}



# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "scrapy_engine.middlewares.ScrapyEngineSpiderMiddleware": 543,
#}


# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   "scrapy_engine.middlewares.ScrapyEngineDownloaderMiddleware": 543,
#    "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 810,   # handles HTTP response compression (i.e. response.body is gibberish for some sites if this is turned on)

    # Disable if you dont want to  follow redirects automatically
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,

}


# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    "scrapy_engine.pipelines.ScrapyEnginePipeline": 300,
#}

'''
# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True # dynamically adjusts the download delay based on server responses. 
# The initial download delay
AUTOTHROTTLE_START_DELAY = 2#5
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 30# 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
HTTPCACHE_ENABLED = False   # True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

LOG_LEVEL='INFO'    # overview of program execution
#LOG_LEVEL='DEBUG'   # detailed information about program execution
#LOG_LEVEL='WARNING' # only show warnings and errors
# LOG_LEVEL='ERROR'   # only show errors
#LOG_LEVEL='CRITICAL' # only show critical errors
#LOG_LEVEL='NOTSET'  # show everything

'''
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# ## ---------------------------------------------------
# ## ---------------- REDIS-SPECIFIC SETTINGS -----------
# ## ---------------------------------------------------
# # Enables scheduling storing requests queue in redis.
# SCHEDULER = "scrapy_redis.scheduler.Scheduler"

# # Ensure all spiders share same duplicates filter through redis.
# DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"

# # Redis Connection URL
# REDIS_URL = 'redis://default:saQznB445IS0AEypcHVpzLKtGTxXilui@redis-13950.c292.ap-southeast-1-1.ec2.cloud.redislabs.com:13950'