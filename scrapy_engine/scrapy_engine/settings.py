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
UPLOAD_SIZE_THRESHOLD_MB = 100.0
UPLOAD_INTERVAL_SECONDS = 3600 # 10 # 1hr is equivaline to 3600 seconds
CHECK_INTERVAL_SECONDS = 300 # 2    # 5 minutes
# ============================================


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "scrapy_engine (+http://www.yourdomain.com)"

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

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "scrapy_engine.middlewares.ScrapyEngineSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "scrapy_engine.middlewares.ScrapyEngineDownloaderMiddleware": 543,
#}

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
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"


LOG_LEVEL='INFO'    # overview of program execution
#LOG_LEVEL='DEBUG'   # detailed information about program execution
#LOG_LEVEL='WARNING' # only show warnings and errors
# LOG_LEVEL='ERROR'   # only show errors
#LOG_LEVEL='CRITICAL' # only show critical errors
#LOG_LEVEL='NOTSET'  # show everything


# ## ---------------------------------------------------
# ## ---------------- REDIS-SPECIFIC SETTINGS -----------
# ## ---------------------------------------------------
# # Enables scheduling storing requests queue in redis.
# SCHEDULER = "scrapy_redis.scheduler.Scheduler"

# # Ensure all spiders share same duplicates filter through redis.
# DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"

# # Redis Connection URL
# REDIS_URL = 'redis://default:saQznB445IS0AEypcHVpzLKtGTxXilui@redis-13950.c292.ap-southeast-1-1.ec2.cloud.redislabs.com:13950'