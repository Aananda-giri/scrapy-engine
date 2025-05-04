# Wayback Machine Web Page Downloader

This script downloads historical web pages from the Wayback Machine (Internet Archive) for a list of websites and their subdomains.


## summary:
 - wayback machine allows bulk downlad of web pages from a list of websites but it basically downloads one snapshot (snapshot is archive of a web page) at a time (you can do multi-threading but it is still one at a time plus the api have rate limits that limits how much rate multi threading you can do).
 

## Features

- Downloads web pages from multiple websites specified in a JSON file
- Automatically discovers and processes subdomains
- Saves historical versions of web pages with timestamps
- Progress tracking with tqdm
- Respects Wayback Machine's rate limits
- Skips already downloaded pages

## Requirements

- Python 3.6+
- Required packages listed in `requirements.txt`

## Installation

1. Clone this repository
2. Install the required packages:
```bash
pip install -r requirements.txt
```

## Usage

1. Edit the `websites.json` file to include the websites you want to download:
```json
{
    "websites": [
        "example.com",
        "github.com",
        "python.org"
    ]
}
```

2. Run the script:
```bash
python wayback_downloader.py
```

The script will:
- Create a `wayback_pages` directory
- Create subdirectories for each domain
- Download all available versions of web pages for each domain and subdomain
- Save the HTML content with timestamps

## Output Structure

```
wayback_pages/
├── example.com/
│   ├── www.example.com/
│   │   ├── 20200101000000.html
│   │   ├── 20200201000000.html
│   │   └── ...
│   └── blog.example.com/
│       ├── 20200101000000.html
│       └── ...
├── github.com/
│   └── ...
└── python.org/
    └── ...
```

## Notes

- The script includes a 1-second delay between requests to be respectful to the Wayback Machine's servers
- Downloaded pages are saved with their timestamps as filenames
- The script will skip any pages that have already been downloaded
- Error handling is included to continue processing even if some downloads fail 