import sqlite3
from datetime import datetime

class URLDatabase:
    def __init__(self, db_path='urls.db'):
        self.conn = sqlite3.connect(db_path, timeout=100)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        # Create 'crawled' table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawled (
                url TEXT PRIMARY KEY,
                timestamp TEXT
            )
        ''')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_crawled_url ON crawled (url)')

        # Create 'to_crawl' table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS to_crawl (
                url TEXT PRIMARY KEY,
                timestamp TEXT
            )
        ''')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_to_crawl_url ON to_crawl (url)')

        self.conn.commit()

    def insert(self, table, url, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow().isoformat()
        try:
            self.cursor.execute(f'INSERT OR IGNORE INTO {table} (url, timestamp) VALUES (?, ?)', (url, timestamp))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")

   

    def bulk_insert(self, table, url_timestamp_pairs, show_progress=True):
        try:
            self.conn.execute('BEGIN')

            # Calculate the step size for progress bar updates
            total = len(url_timestamp_pairs)
            step = total // 100 if total > 100 else 1

            for i, (url, timestamp) in enumerate(url_timestamp_pairs):
                try:
                    print(f'inserting {url} into {table}')
                    self.cursor.execute(f'INSERT OR IGNORE INTO {table} (url, timestamp) VALUES (?, ?)', (url, timestamp))
                except sqlite3.Error as e:
                    print(f"Failed to insert {url} into {table}: {e}")

                if show_progress:
                    # Update the progress bar every 'step' items
                    if i % step == 0:
                        print(f"Progress: {i / total * 100:.2f}%")

            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"Bulk insert transaction failed: {e}")

    def exists(self, table, url):
        self.cursor.execute(f'SELECT 1 FROM {table} WHERE url = ?', (url,))
        return self.cursor.fetchone() is not None

    def fetch(self, table, n):
        self.cursor.execute(f'SELECT url, timestamp FROM {table} LIMIT ?', (n,))
        return [(row[0], row[1]) for row in self.cursor.fetchall()]

    def fetch_all(self, table):
        self.cursor.execute(f'SELECT url, timestamp FROM {table}')
        return [(row[0], row[1]) for row in self.cursor.fetchall()]

    def delete(self, table, urls):
        if isinstance(urls, str):
            urls = [urls]
        elif isinstance(urls, list):
            if isinstance(urls[0], tuple):
                urls = [url[0] for url in urls]
        try:
            self.conn.execute('BEGIN')
            self.cursor.executemany(f'DELETE FROM {table} WHERE url = ?', ((url,) for url in urls))
            # self.cursor.executemany(f'DELETE FROM {table} WHERE url = ?', ((url,) for url in urls))
            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"Deletion failed: {e}")
    def count_entries(self, table):
        self.cursor.execute(f'SELECT COUNT(*) FROM {table}')
        return self.cursor.fetchone()[0]

    def close(self):
        self.conn.close()

# Usage example:
if __name__ == "__main__":
    db = URLDatabase()

    # Inserting a new URL into 'crawled' table with the current timestamp
    db.insert('crawled', 'http://example.com')

    # Inserting a new URL into 'to_crawl' table with a specific timestamp
    db.insert('to_crawl', 'http://example.org', '2024-05-27T12:34:56Z')

    # Bulk inserting multiple URLs into 'crawled' table
    crawled_urls = [
        ('http://example.net', '2024-05-27T12:34:56Z'),
        ('http://example.edu', '2024-05-27T12:34:56Z'),
        ('http://example.xyz', '2024-05-27T12:34:56Z'),
        # Add more URL, timestamp pairs
    ]
    db.bulk_insert('crawled', crawled_urls)

    import time
    # Bulk inserting multiple URLs into 'to_crawl' table
    to_crawl_urls = [
        ('http://example.com', time.time()),
        ('http://example.co.uk', time.time()),
        ('http://example.fr', time.time()),
        ('http://fuckme.org', time.time()),
        ('http://example.de', time.time()),
        ('http://nofuckme.org', time.time()),
        # Add more URL, timestamp pairs
    ]
    db.bulk_insert('to_crawl', to_crawl_urls)

    # Getting URLs to crawl
    # urls_to_crawl = db.fetch('to_crawl', 3)
    print(f'to_crawl: {db.fetch_all("to_crawl")}')  # Output: List of 3 URLs from 'to_crawl' table

    # Deleting URLs from both tables
    # db.delete('to_crawl', ['http://example.com', 'http://example.org', 'invalid_url'])
    # db.delete('to_crawl', [('http://example.com','sth'), ('http://example.org', 'seteht'), ('invalid_url','jfsal')])
    print(f'to_crawl after delete: {db.fetch_all("to_crawl")}')  # Output: List of 3 URLs from 'to_crawl' table
    print(f"exists to_crawl: {db.exists('crawled', 'http://example.com')}")  # Output: True
    print(f"exists to_crawl: {db.exists('to_crawl', 'http://example.org')}")  # Output: False

    
    
