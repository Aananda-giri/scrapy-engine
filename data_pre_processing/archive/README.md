# Data pre process archive
* Contains data pre-processing code old data format of `crawled_data`
```
    # old-format (code in archive):
      {
        <url-11>:{'page_title': 'page-1', 'paragraph': 'devanagari-paragraph-1'},
        <url-1>:{'page_title': page-1' 'paragraph': 'devanagari-paragraph-2'},
        <url-2>:{'page_title': page-2, 'paragraph': 'devanagari-paragraph-3'}
      }
    # new-format:
      {
        <url-11>:{'page_title': 'page-1', 'paragraphs': ['devanagari-paragraph-1', 'devanagari-paragraph-2']},
        <url-2>:{'page_title': page-2, 'paragraphs': ['devanagari-paragraph-2', 'devanagari-paragraph-3']}
      }

      * paragraphs are jsonified list i.e. saved after performing `json.dumps(paragraphs)`
```