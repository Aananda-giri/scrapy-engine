import requests
from bs4 import BeautifulSoup as Soup
import os
import csv

from urllib.parse import urljoin, urlparse

# Constants for the attributes to be extracted from the sitemap.
ATTRS = ["loc", "lastmod", "priority"]

def parse_sitemap(sitemap_url):
    """Parse the sitemap at the given URL and append the data to a CSV file."""

    # Return False if the URL is not provided.
    if not sitemap_url:
        return False
    
    # base_url + .csv
    csv_filename = sitemap_url.replace("https://", "").replace("http://", "").replace("www.", "").split('/')[0] + ".csv"

    # Attempt to get the content from the URL.
    response = requests.get(sitemap_url)
    # Return False if the response status code is not 200 (OK).
    if response.status_code != 200:
        return False

    # Parse the XML content of the response.
    soup = Soup(response.content, "xml")

    # Recursively parse nested sitemaps.
    for sitemap in soup.find_all("sitemap"):
        '''
        loc is redirect url given by sitemap.
        sometimes it is full url (e.g. see here: https://www.janaboli.com/sitemap_index.xml)
        other times it is relative url like 'sitemap/category' (see here: https://kantipurtv.com/sitemap)
        
        so we need to join the base_url with the loc to get the full url.
        urljoin returns redirect_url in case redirect url is full url, so we would not even check if it is full url or not.
        
        ```
            # example
            from urllib.parse import urljoin

            # base url is current url
            base_url = "https://kantipurtv.com/sitemap"
            redirect_url = "sitemap/news/1"

            full_url = urljoin(base_url, redirect_url)
            print(full_url) # https://kantipurtv.com/sitemap/news/1
        ```
        '''
        loc = sitemap.find("loc").text
        print(loc)
        parse_sitemap(urljoin(sitemap_url, loc))  # url is current url and loc is redirect url given by sitemap

    # root directory is the directory of this fileq
    root = os.path.dirname(os.path.abspath(__file__))

    # Find all URL entries in the sitemap.
    urls = soup.find_all("url")
    # print(urls)

    rows = []
    for url in urls:
        row = []
        for attr in ATTRS:
            found_attr = url.find(attr)
            # Use "n/a" if the attribute is not found, otherwise get its text.
            row.append(found_attr.text if found_attr else "n/a")
            if row[0] != "n/a":
                # save full url
                row[0] = urljoin(sitemap_url, row[0])
        rows.append(row)

    # Check if the file already exists
    file_exists = os.path.isfile(os.path.join(root, csv_filename))

    # Append the data to the CSV file.
    with open(os.path.join(root, csv_filename), "a+", newline="") as csvfile:
        print(f'saved: {csv_filename}')
        writer = csv.writer(csvfile)
        # Write the header only if the file doesn't exist
        if not file_exists:
            writer.writerow(ATTRS)
        writer.writerows(rows)
    return True

# Example usage
# parse_sitemap("https://www.janaboli.com/post-sitemap.xml")





our_unique_urls = [

        # no clean sitemap
        'https://kantipurtv.com/',


        # sitemap: "https://www.janaboli.com/post-sitemap.xml",
        'https://www.janaboli.com/',


        # no sitemap but seems to have article pages in format: https://ekagaj.com/article/tourism/44399/ <1 to 44399 each number dentoting one article>
        'https://ekagaj.com/',


        # no sitemap
        'https://swasthyakhabar.com/',

        # # only follow post/ ?
        # "https://deshsanchar.com/sitemap_index.xml",
        'https://deshsanchar.com/',

        # no site map for non admins
        'https://www.ukaalo.com/',

        # could not find sitemap or robots.txt
        'https://www.ukeraa.com/',

        # "https://cijnepal.org.np/post-sitemap.xml",
        'https://cijnepal.org.np/',

        # sitemap sucks
        'https://www.dekhapadhi.com/',

        # no sitemap
        # english: https://nepaltvonline.com/content/en-news/
        'https://nepaltvonline.com/',


        # sitemap: https://bizmandu.com/sitemap_index.xml
        # filter post sitemap?
        'https://bizmandu.com/',

        # no sitemap
        'https://www.news24nepal.com/',

        # no sitemap
        'https://www.ajakoartha.com/',


        # https://nepalipaisa.com/sitemap.xml
        'https://nepalipaisa.com/',


        # it sucks
        # https://www.aakarpost.com/sitemap.xml
        'https://www.aakarpost.com/',

        # https://aarthiknews.com/sitemap.xml
        'https://aarthiknews.com/',

        # no sitemap
        'https://arthasansar.com/',

        'https://himalpress.com/',

        'https://newspolar.com',

        'https://www.merolifestyle.com/',

        'https://www.corporatenepal.com/',

        'https://halokhabar.com/',

        'https://www.sutranews.com/',

        'https://clickmandu.com/',

        'https://www.nepalpage.com/',

        'https://nepalbahas.com/',

        'https://arthadabali.com/',

        'https://nagariknews.nagariknetwork.com/',

         #
        'https://laganisutra.com/',
        
        #  have sitemap
        # (short articles)
        'https://neplays.com/',
        # (short articles) error generating sitemap
        'https://rashtriyadainik.com/'
    ]

for url in our_unique_urls:
    # print(url, end=' \n\n')
    print('.', end='')
    have_sitemap = parse_sitemap(url + 'sitemap.xml')
    
    if not have_sitemap:
        print(f'no_sitemap: {url}')
