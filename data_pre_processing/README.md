# Data pre process

```
    # crawled_data-format:
      {
        <url-11>:{'page_title': 'page-1', 'paragraphs': ['devanagari-paragraph-1', 'devanagari-paragraph-2']},
        <url-2>:{'page_title': page-2, 'paragraphs': ['devanagari-paragraph-2', 'devanagari-paragraph-3']}
      }

      * paragraphs are jsonified list i.e. saved after performing `json.dumps(paragraphs)`
```