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


### Crawled_data
format:
```
{
    'parent_url': response.url,
    'page_title': response.css('title::text').get(),
    'paragraph': paragraph,
    # 'is_nepali_confidence': confidence    # data from previous version of the code might contain this field
}
```

e.g.
```
{
    'parent_url': 'https://www.bbc.com/nepali',,
    'page_title': 'मुख पृष्ठ - BBC News नेपाली',
    'paragraph': 'नेपालले करिब एक महिना अघि नै औपचारिक पत्र पठाएर जीबी राईलाई स्वदेश फर्काइदिन गरेको आग्रहबारे मलेशियाले कुनै औपचारिक जबाफ दिएको छैन।',
}

```


### other_data
1. Data if scrapy_engine response is not text/html
syntax:
```
{
    'url': response.url,
    'Content-Type': response.headers.get('Content-Type', '').decode('utf-8'),
}
```
e.g.
```
{
    'url': 'https://www.nrb.org.np/contents/uploads/2022/03/SP-4-Text-for-website.pdf',
    'Content-Type': 'application/pdf',
}
```


2. Social media links
syntax:
```
{
    'parent_url': response.url,
    'url': link.url,
    'text': link.text,
    'link_type': "social_media",
    'link_description': social_media_type
}
```

3. Drive link
syntax:
```
{
    'parent_url': response.url,
    'url': link.url,
    'text': link.text,
    'link_type': "drive_link",
    'link_description': None
}
```

4. Document links
syntax:
```
{
    'parent_url': response.url,
    'url': link.url,
    'text': link.text,
    'link_type': "document",
    'link_description': document_type
}
```

e.g. 
```
{
    'parent_url': <some url>,
    'url': 'https://www.nrb.org.np/contents/uploads/2022/03/SP-4-Text-for-website.pdf',
    'text': <some text string>,
    'link_type': "document",
    'link_description': "document"      # pdf is defined under category of document in functions.is_document_link
}
```