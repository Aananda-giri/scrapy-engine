from urllib.parse import urlparse
import re
from typing import List, Set

from typing import Optional
import re
import tldextract

class NepaliUrlValidator:
    def __init__(self):
        '''
            pre-defined site specific patterns/rules for inentifying Nepali content
            e.g. bbc only contains nepali content under /nepali section

            # format for site_patterns
            {
                <tldextract.extract(url).registered_domain>: [<site_pattern1>, <site_pattern2>, ...],
                ...
            }

            
            # Patterns match using regex

            pattern: 
                (?!...) syntax means NOT operation of reges inside the brackets
                ^ is start of the string
                $ is end of string
                .* matches any character 0 or more times
                \/ is / <forward slash> with escape_character \  <backward slash> in front
                \. is . <dot> with escape_character \  <backward slash> in front
        '''
        # Define patterns for different news sites
        # Format: {domain: [pattern1, pattern2, ...]}
        self.site_patterns = {
            'bbc.co.uk': [
                r'^.*\/nepali\/.*$'
                # r'/nepali(?:/|$)'
            ],
            "ekagaj.com": [r'^(?!.*en\.).*$'],          # avoid https://en.ekagaj.com/
            "himalpress.com": [r'^(?!.*en\.).*$'],  # avoid # avoid https://en.himalpress.com/govt-spent-rs-107-66-billion-on-agriculture-subsidies-in-past-five-years/
            "nepalbahas.com": [r'^(?!.*en\.).*$'],
            "nayapage.com": [r'^(?!.*en\.).*$'],
            "nepalkhabar.com": [r'^(?!.*en\.).*$'],
            "setopati.com": [r'^(?!.*en\.).*$'],
            
            "nepalgunjnews.com": [r'^(?!.*\/english\/).*$'],                      # avoid https://www.nepalgunjnews.com/english/20230868906/
            "bbc.com": [r'^.*\/nepali\/.*$'],                                     # only follow /nepali ()
            
            "deshsanchar.com": [r'^(?!.*\/english\.).*$'],   # avoid https://english.deshsanchar.com/nepal-india-jbf-meeting-emphasis-on-expansion-of-bilateral-trade/
            "aarthiknews.com": [r'^(?!.*\/english\.).*$'],   # avoid https://english.aarthiknews.com/news/detail/17669/
            "corporatenepal.com": [r'^(?!.*\/english\.).*$'], # avoid https://english.merolifestyle.com/?p=2192
            "nepalpage.com": [r'^(?!.*\/english\.).*$'],           # avoid https://english.nepalpage.com/2022/12/like-walking-on-missiles-us-airman-recalls-the-horror-of-the-vietnam-christmas-bombings-50-years-on/
            "lokaantar.com": [r'^(?!.*\/english\.).*$'],           # avoid https://english.lokaantar.com/news/detail/33475/
            "dhangadhikhabar.com": [r'^(?!.*\/english\.).*$'],   # avoid https://english.dhangadhikhabar.com/news/73691
            "khabarhub.com": [r'^(?!.*\/english\.).*$'],               # avoid https://english.khabarhub.com/2025/12/427719/
            "pardafas.com": [r'^(?!.*\/english\.).*$'],
            "makalukhabar.com": [r'^(?!.*\/english\.).*$'],
            "kathmandupati.com": [r'^(?!.*\/english\.).*$'],
            "annapurnapost.com": [r'^(?!.*\/english\.).*$'],
            "madheshvani.com": [r'^(?!.*\/english\.).*$'],
            "nepalwatch.com": [r'^(?!.*\/english\.).*$'],
            "dcnepal.com": [r'^(?!.*\/english\.).*$'],
            "karobardaily.com": [r'^(?!.*\/english\.).*$']
        }
    
    def is_probable_nepali_content_url(self, url: str) -> bool:
        """
        Check if the given URL contains Nepali content based on predefined patterns.
        return True if url is not in predefined patterns
        Args:
            url (str): The URL to check
            
        Returns:
            bool: whether it is a probable Nepali content URL, None otherwise
        """
        try:
            domain = tldextract.extract(url).registered_domain

            # First check if we have patterns for this domain
            if domain in self.site_patterns:
                # print(f' domain: {domain}, path:{path}')
                # Check if URL matches any of the site's Nepali content patterns
                for pattern in self.site_patterns[domain]:
                    if re.search(pattern, url):
                        return True
                    else:
                        return False
            else:
                # print(f'patterns not defined for {domain}')
                # If no patterns are defined, return True (probable nepali content)
                return True
        except Exception as e:
            print(f"Error processing URL {url}: {str(e)}")
            return None
    
    def get_all_sites(self) -> List[str]:
        """
        Get list of all supported sites.
        
        Returns:
            List[str]: List of supported netlocs
        """
        return list(self.site_patterns.keys())


class WebPageURLFilter:
    '''
    * we do not want to crawl image/other file urls (i.e. urls that are not likely to contain html content)
    '''
    def __init__(self):
        self.nepali_url_validator = NepaliUrlValidator()

        # Common file extensions that typically don't contain useful text
        self.non_webpage_extensions: Set[str] = {
            # Images
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico', '.tiff', '.tif', '.pct', '.psp',
            
            # Documents/Downloads
            '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx', '.csv', '.odp', '.pps', '.ods',
            '.zip', '.rar', '.tar', '.gz', '.7z', '.7zip', '.bz2', '.tar.gz', '.xz',
            
            '.odt', '.pst', '.ai', '.drw', '.dxf', '.eps', '.ps', '.cdr', '.odg',
            
            # Audio/Video
            '.mp3', '.wav', '.ogg', '.m4a', '.aac', '.wma', '.ra', '.mid', '.au', '.aiff', '.3gp', '.asf', '.asx',
            '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mkv', '.mng', '.mpg', '.qt', '.rm', '.swf',
            
            # Programming/Data
            '.js', '.css', '.json', '.xml', '.rss', '.atom',
            '.woff', '.woff2', '.ttf', '.eot',  # Fonts
            '.sql', '.db', '.sqlite',
            
            # Other binary/non-text files
            '.exe', '.dll', '.bin', '.iso', '.dmg', '.apk', '.ipa'
        }

        
        # API and non-webpage URL patterns
        self.non_webpage_patterns = [
            # API endpoints
            r'/api/.*',
            r'/v\d+/.*',  # API versions
            r'/rest/.*',
            r'/graphql.*',
            r'/ws/.*',    # WebSocket
            
            # Authentication/user management
            r'/oauth/.*',
            r'/login/.*',
            r'/logout/.*',
            r'/signin/.*',
            r'/signup/.*',
            
            # Common static asset paths
            r'/static/.*',
            r'/assets/.*',
            r'/dist/.*',
            r'/build/.*',
            r'/images/.*',
            r'/img/.*',
            r'/css/.*',
            r'/js/.*',
            r'/fonts/.*',
            
            # Admin/backend paths
            r'/admin/.*',
            r'/wp-admin/.*',
            r'/wp-content/.*',
            r'/wp-includes/.*',
            r'/phpmyadmin/.*',
            r'/cpanel/.*',
            
            # Common CDN patterns
            r'cdn\.',
            r'\.cloudfront\.net',
            r'\.akamai\.net',
            
            # Common tracking/analytics
            r'/pixel/.*',
            r'/tracking/.*',
            r'/analytics/.*',
            r'/stats/.*',
            
            # Feed URLs
            r'/feed/.*',
            r'/rss/.*',
            r'/atom/.*',
            
            # Common non-webpage endpoints
            r'/download/.*',
            r'/uploads/.*',
            r'/thumb/.*',
            r'/thumbnail/.*',
            r'/print/.*',
            r'/raw/.*'
        ]
        
        # Compile patterns for better performance
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) 
                                for pattern in self.non_webpage_patterns]

    def is_likely_webpage(self, url: str) -> bool:
        """
        Check if a URL is likely to point to a webpage with useful text content.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if the URL likely points to a webpage, False otherwise
        """
        try:
            # Parse URL
            parsed = urlparse(url)
            
            # Skip empty or invalid URLs
            if not parsed.netloc or not parsed.scheme:
                return False
            
            # Skip non-HTTP(S) protocols
            if parsed.scheme not in ('http', 'https'):
                return False
            
            # Check file extension
            path = parsed.path.lower()
            if any(path.endswith(ext) for ext in self.non_webpage_extensions):
                return False
            
            # Check for non-webpage patterns
            if any(pattern.search(url) for pattern in self.compiled_patterns):
                return False
            
            # Additional checks for common cases
            path_parts = path.split('/')
            for part in path_parts:
                # # Skip URLs with numeric IDs in path (often API endpoints)
                # if part.isdigit() and len(part) > 6:
                #     return False
                    
                # Skip URLs with very long random-looking segments
                if len(part) > 90:
                    return False
                
                # Skip URLs with base64-looking segments
                if len(part) > 30 and re.match(r'^[A-Za-z0-9+/]+={0,2}$', part):
                    return False
            
            return self.nepali_url_validator.is_probable_nepali_content_url(url)
            
        except Exception:
            return False

    def filter_urls(self, urls: List[str]) -> List[str]:
        """
        Filter a list of URLs to keep only those likely to be webpages.
        
        Args:
            urls: List of URLs to filter
            
        Returns:
            List of URLs that are likely to be webpages
        """

        
        return [url for url in urls if self.is_likely_webpage(url)]




if __name__ == "__main__":
    # Test the filter
    def test_url_filter():
        filter = WebPageURLFilter()
        
        test_cases = [
            # Should be accepted (webpages)
            ("https://example.com/blog/article", True),
            ("https://example.com/about", True),
            ("https://example.com/products/shoes", True),
            ("https://example.com/category/electronics", True),
            
            # Should be rejected (non-webpages)
            ("https://example.com/image.jpg", False),
            ("https://example.com/document.pdf", False),
            ("https://api.example.com/v1/users", False),
            ("https://example.com/static/main.css", False),
            ("https://cdn.example.com/asset.js", False),
            ("https://example.com/wp-admin/settings", False),
            ("https://example.com/download/file.zip", False),
            ("https://example.com/12345678", False),
            ("https://example.com/api/data", False),
            ("https://example.com/assets/logo.png", False),
            ("ftp://example.com/file", False),
            ("mailto:user@example.com", False),
            
            # Edge cases
            ("https://example.com", True),
            ("https://blog.example.com", True),
            ("https://example.com/page-with-image.html", True),
            ("https://example.com/article-123", True),
            ("https://example.com/very-long-base64-looking-string-that-should-be-rejected" + "a" * 50, False),
        ]
        
        for url, expected in test_cases:
            result = filter.is_likely_webpage(url)
            assert result == expected, f"\nURL: {url}\nExpected: {expected}\nGot: {result}"
    test_url_filter()
