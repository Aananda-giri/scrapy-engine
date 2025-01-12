from datetime import datetime
import hashlib
import os
import pickle

class PickleUtils:
    @staticmethod
    def _generate_url_hash(url: str) -> str:
        """Generate MD5 hash of URL."""
        return hashlib.md5(url.encode()).hexdigest()
    # print(generate_url_hash('https://example.com/'))    # 182ccedb33a9e03fbf1079b209da1a31
    
    @staticmethod
    def _get_file_name(url: str) -> str:
        """
        Create filename with hash and timestamp.
        
        url_hash: MD5 hash of URL
        timestamp: current timestamp
        filename: {url_hash}_{timestamp}_temp.pickle
        """
        url_hash = PickleUtils._generate_url_hash(url)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        suffix = "_temp.pickle"
        return f"{url_hash}_{timestamp}{suffix}"
    
    @staticmethod
    def save_html(response_url, request_url, response_body, redirect_links, pickle_directory='pickles'):
        '''

            * save html data in filename: hash(url) + _temp.pickle
            * move _temp.pickle -> pickles/<filename>.pickle
        '''
        # 1) get filename
        filename = PickleUtils._get_file_name(response_url)
        
        # 2) save data to <filename>_temp.pickle
        data = {
            'request_url':request_url,
            'response_url':response_url,
            'response_body':response_body,
            'redirect_links':redirect_links
            }
        # save to pickle file
        with open(filename, 'wb') as f:
            pickle.dump(data, f)
        
        os.makedirs(pickle_directory, exist_ok=True)
        
        # 3) rename <filename>_temp.pickle to <filename>.pickle
        os.rename(filename, pickle_directory + '/' + filename.replace('_temp', ''))
    
    def load_pickle(filename=None):
        if not filename:
            filename="fa5b40e417c5cb81fb5c31d6ba6903da_20250110_212913_581403.pickle"
        with open(filename, 'rb') as f:
            data = pickle.load(f)
        return data

if __name__ == '__main__':
    pickle_utils = PickleUtils
    PickleUtils._get_file_name('https://example.com/')   # '182ccedb33a9e03fbf1079b209da1a31_20250107_152420_350083_temp.pickle'