from urllib.parse import urlparse
import re
from typing import List, Set

class WebPageURLFilter:
    '''
    * we do not want to crawl image/other file urls (i.e. urls that are not likely to contain html content)
    '''
    def __init__(self):
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
            
            return True
            
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
