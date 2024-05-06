import scrapy
# scrapy crawl quotes_spider -o quotes.json
class QuotesSpider(scrapy.Spider):
    name = "quotes_spider"
    start_urls = [
        "https://quotes.toscrape.com/page/1/",
        "https://quotes.toscrape.com/page/2/",
        # "https://quotes.toscrape.com/page/3/",
        # "https://quotes.toscrape.com/page/4/",
        # "https://quotes.toscrape.com/page/5/",
        # "https://quotes.toscrape.com/page/6/",
        # "https://quotes.toscrape.com/page/7/",
        # "https://quotes.toscrape.com/page/8/",
        # "https://quotes.toscrape.com/page/9/",
        # "https://quotes.toscrape.com/page/10/",
    ]

    def parse(self, response):
        import os;print('\n\n\n\n', os.listdir(), os.environ.get('some_key2'), '\n\n')
        # print(f"\n before {self.crawler.engine.slot.scheduler.stats._stats['scheduler/enqueued']} \n\n")
        print(f"\n before2 {len(self.crawler.engine.slot.inprogress)}")  # dont know why it always returns zero
        print(self.crawler.engine.settings.get('CONCURRENT_REQUESTS'))
        print(len(self.crawler.engine.slot.inprogress) >= self.crawler.engine.settings.get('CONCURRENT_REQUESTS'))
        for quote in response.css("div.quote"):
            yield {
                "text": quote.css("span.text::text").get(),
                "author": quote.css("small.author::text").get(),
                "tags": quote.css("div.tags a.tag::text").getall(),
            }
            # next_page = response.css("li.next a::attr(href)").get()
            # if next_page is not None:
            #     next_page = response.urljoin(next_page)
            #     yield scrapy.Request(next_page, callback=self.parse)
        # print(f"\n After {self.crawler.engine.slot.scheduler.stats._stats['scheduler/enqueued']} \n\n")
        # print(f"\n after2 {len(self.crawler.engine.slot.scheduler)}") # dont know why it always returns zero
    def spider_closed(self, spider, reason):
        print(f"almost sakyo muji. reason:{reason}")
        
        # Restart spider  with more start urls
        new_urls = get_new_start_urls()
        for url in new_urls:
            yield scrapy.Request(url, callback=self.parse)

all_start_urls = [
        # "https://quotes.toscrape.com/page/1/",
        # "https://quotes.toscrape.com/page/2/",
        "https://quotes.toscrape.com/page/3/",
        "https://quotes.toscrape.com/page/4/",
        "https://quotes.toscrape.com/page/5/",
        "https://quotes.toscrape.com/page/6/",
        "https://quotes.toscrape.com/page/7/",
        "https://quotes.toscrape.com/page/8/",
        "https://quotes.toscrape.com/page/9/",
        "https://quotes.toscrape.com/page/10/"
        ]

def get_new_start_urls():
    new_start_urls = random.sample(all_start_urls, 2)
    for url in new_start_urls:
        all_start_urls.remove(url)
    return new_start_urls