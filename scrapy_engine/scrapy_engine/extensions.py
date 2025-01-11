from scrapy import signals
import threading
from typing import Type
from scrapy.crawler import Crawler
from .background_upload import BackgroundUploadService
import logging

class BackgroundUploadExtension:
    """Scrapy extension to manage background upload service."""
    
    def __init__(self, crawler: Crawler):
        self.crawler = crawler
        self.logger = logging.getLogger(__name__)
        self.background_service = None
        self.upload_thread = None
        
        # Get settings from crawler with defaults
        self.settings = {
            'pickle_dir': crawler.settings.get('PICKLE_DIR', './'),
            'size_threshold_mb': crawler.settings.get('UPLOAD_SIZE_THRESHOLD_MB', 100.0),
            'upload_interval_seconds': crawler.settings.get('UPLOAD_INTERVAL_SECONDS', 3600),
            'check_interval_seconds': crawler.settings.get('CHECK_INTERVAL_SECONDS', 300)
        }

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        """Create extension from crawler."""
        ext = cls(crawler)
        
        # Connect the extension methods to Scrapy signals
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        
        return ext

    def spider_opened(self, spider):
        """Start the background service when spider starts."""
        self.logger.info("Starting background upload service...")
        
        try:
            # Initialize the service with settings
            self.background_service = BackgroundUploadService(
                pickle_dir=self.settings['pickle_dir'],
                size_threshold_mb=self.settings['size_threshold_mb'],
                upload_interval_seconds=self.settings['upload_interval_seconds'],
                check_interval_seconds=self.settings['check_interval_seconds']
            )
            
            # Start the service in a daemon thread
            self.upload_thread = threading.Thread(
                target=self.background_service.run,
                name='BackgroundUploadThread'
            )
            self.upload_thread.daemon = True
            self.upload_thread.start()
            
            self.logger.info("Background upload service started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start background upload service: {str(e)}")
            # Optionally, you might want to stop the spider if this is critical
            # self.crawler.engine.close_spider(spider, 'Failed to start upload service')

    def spider_closed(self, spider):
        """Cleanup when spider is closing."""
        self.logger.info("Cleaning up background upload service...")
        if self.background_service:
            # Optionally add a stop method to your service class
            if hasattr(self.background_service, 'stop'):
                self.background_service.stop()
            
            # If you need to wait for the thread to finish
            if self.upload_thread and self.upload_thread.is_alive():
                self.upload_thread.join(timeout=60)  # Wait up to 60 seconds
                
        self.logger.info("Background upload service cleanup completed")