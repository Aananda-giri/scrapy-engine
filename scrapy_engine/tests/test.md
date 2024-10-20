```
scrapy shell "/home/anon/weekly-projects/scrapy_engine/scrapy_engine/tests/index.html"
headings_and_paragraphs = response.css('h1::text, h2::text, h3::text, h4::text, h5::text, h6::text, p::text, span::text, div::text').getall()  # , a::text, body *::text'
headings_and_paragraphs
```
