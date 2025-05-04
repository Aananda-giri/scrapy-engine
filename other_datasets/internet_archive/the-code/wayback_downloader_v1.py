# 
'''
# Download one page at a time
  * issue: too slow to download one page at a time
  * connection refused errors (probably due to rate limits of wayback api.)
'''


import json
import os
from datetime import datetime
import requests
from waybackpy import WaybackMachineCDXServerAPI
from tqdm import tqdm
import time

from dotenv import load_dotenv
load_dotenv()


class WaybackDownloader:
    def __init__(self, json_file):
        self.json_file = json_file
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        self.output_dir = "wayback_pages"

        self.start_timestamp=int(os.environ.get("START_TIMESTAMP",2016))
        self.end_timestamp=int(os.environ.get("END_TIMESTAMP",2017))
        self.we_should_get_subdomains = str(os.environ.get("GET_SUBDOMAINS", True)).lower() == "true"

        print(f"Start timestamp: {self.start_timestamp}")
        print(f"End timestamp: {self.end_timestamp}")
        print(f"Get subdomains: {self.we_should_get_subdomains}")
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def load_websites(self):
        """Load websites from JSON file."""
        with open(self.json_file, 'r') as f:
            data = json.load(f)
        return data['websites']

    def get_subdomains(self, domain):
        """Get all subdomains for a given domain using Wayback Machine."""
        cdx_api = WaybackMachineCDXServerAPI(domain, user_agent=self.user_agent)
        # example domain: setopati.com

        # Get all snapshots
        snapshots = cdx_api.snapshots()
        
        # Extract unique subdomains
        subdomains = set()
        for snapshot in snapshots:
            url = snapshot.archive_url
            '''
            # Example urls
            https://web.archive.org/web/20250504022742/https://www.setopati.com/

            '''


            # Extract the original URL from the archive URL
            # Format: https://web.archive.org/web/TIMESTAMP/http://original-url
            original_url = url.split('/web/')[1].split('/', 1)[1]
            if original_url.startswith('http://'):
                original_url = original_url[7:]  # Remove 'http://'
            elif original_url.startswith('https://'):
                original_url = original_url[8:]  # Remove 'https://'
            
            # Get the domain part
            subdomain = original_url.split('/')[0]
            subdomains.add(subdomain)

            '''
            example subdomains
            {'setopati.com', 'setopati.com:80', 'www.setopati.com', 'www.setopati.com:80'}
            '''
        
        return list(subdomains)

    def download_page(self, url, timestamp):
        """
        Download a specific snapshot of single webpage.

        e.g. webpage: https://web.archive.org/web/20240101114959/https://www.setopati.com
             timestamp: 20240101114959
        """
        try:
            
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return response.text
            return None
        except Exception as e:
            print(f"Error downloading {url}: {str(e)}")
            return None

    def process_website(self, domain):
        """Process a single website and its subdomains."""
        print(f"\nProcessing domain: {domain}")
        
        # Create domain directory
        domain_dir = os.path.join(self.output_dir, domain)
        if not os.path.exists(domain_dir):
            os.makedirs(domain_dir)

        # Get subdomains
        if self.we_should_get_subdomains:
            subdomains = self.get_subdomains(domain)
            # Example subdomains: ['setopati.com', 'setopati.com:80', 'www.setopati.com', 'www.setopati.com:80']
            print(f"Found {len(subdomains)} subdomains")
        else:
            subdomains = [domain]
            print(f"Domain: {domain}")

        

        for subdomain in tqdm(subdomains, desc="Processing subdomains"):
            cdx_api = WaybackMachineCDXServerAPI(subdomain, user_agent=self.user_agent, start_timestamp=self.start_timestamp, end_timestamp=self.end_timestamp)
            
            try:
                # Get snapshots
                snapshots = cdx_api.snapshots()
                
                # Create subdomain directory
                subdomain_dir = os.path.join(domain_dir, subdomain)
                if not os.path.exists(subdomain_dir):
                    os.makedirs(subdomain_dir)

                # Download each snapshot
                for snapshot in snapshots:
                    '''
                    # Example snapshot
                        digest:OU3KI6ABLDLIWHCLN27IDHB2W75H2WSK 
                        mimetype:text/html 
                        statuscode:200 
                        urlkey:com,setopati)/ 
                        datetime_timestamp:2024-01-01 11:49:59 
                        length:19064 
                        original:https://www.setopati.com/ 
                        timestamp:20240101114959
                    '''
                    timestamp = snapshot.timestamp
                    # example: 20240101114959

                    archive_url = snapshot.archive_url
                    # example: https://web.archive.org/web/20240101114959/https://www.setopati.com/
                    
                    # Create filename
                    filename = f"{timestamp}.html"
                    filepath = os.path.join(subdomain_dir, filename)
                    
                    # Skip if file already exists
                    if os.path.exists(filepath):
                        continue
                    
                    # Download and save the page
                    content = self.download_page(archive_url, timestamp)
                    if content:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(content)
                    
                    # Be nice to the Wayback Machine
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Error processing {subdomain}: {str(e)}")
                continue

    def run(self):
        """Main execution method."""
        websites = self.load_websites()
        print(f"Loaded {len(websites)} websites from {self.json_file}")
        
        for website in websites:
            self.process_website(website)

if __name__ == "__main__":
    downloader = WaybackDownloader("websites.json")
    downloader.run() 