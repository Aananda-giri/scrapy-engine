import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import time
import os, sys
import json

from pathlib import Path
sys.path.append(str(Path('../server/urls/').resolve()))

from server import URLManager

class TestURLManager(unittest.TestCase):
    def setUp(self):
        # Mock MongoDB collection
        self.mock_collection = MagicMock()
        self.url_manager = URLManager(self.mock_collection)

        # Mock bloom filters
        self.url_manager.crawled_bloom = MagicMock()
        self.url_manager.error_bloom = MagicMock()

    def test_add_url_success(self):
        self.mock_collection.find_one.return_value = None
        self.url_manager.crawled_bloom.check.return_value = False
        self.url_manager.error_bloom.check.return_value = False

        result = self.url_manager.add_url("http://example.com")
        self.assertTrue(result)
        self.mock_collection.insert_one.assert_called_once()

    def test_add_url_failure_long_url(self):
        long_url = "http://" + "a" * 300
        result = self.url_manager.add_url(long_url)
        self.assertFalse(result)

    def test_add_url_failure_existing_url(self):
        self.mock_collection.find_one.return_value = {"url": "http://example.com"}
        result = self.url_manager.add_url("http://example.com")
        self.assertFalse(result)

    def test_get_url_to_crawl(self):
        self.mock_collection.find_one_and_update.return_value = {"url": "http://example.com"}
        url = self.url_manager.get_url_to_crawl()
        self.assertEqual(url, "http://example.com")

    def test_get_urls_to_crawl(self):
        mock_docs = [{"_id": 1, "url": "http://example1.com"}, {"_id": 2, "url": "http://example2.com"}]
        self.mock_collection.find.return_value = mock_docs
        self.mock_collection.find_one_and_update.side_effect = [
            {"url": "http://example1.com"},
            {"url": "http://example2.com"}
        ]

        urls = self.url_manager.get_urls_to_crawl(2)
        self.assertEqual(urls, ["http://example1.com", "http://example2.com"])
        self.assertEqual(self.mock_collection.find_one_and_update.call_count, 2)

    def test_mark_url_crawled(self):
        self.url_manager.mark_url_crawled("http://example.com")
        self.mock_collection.delete_one.assert_called_once_with({"url": "http://example.com"})
        self.url_manager.crawled_bloom.add.assert_called_once_with("http://example.com")

    def test_check_timeout_urls(self):
        timed_out_docs = [
            {"_id": 1, "url": "http://example1.com", "crawling_count": 3},
            {"_id": 2, "url": "http://example2.com", "crawling_count": 1}
        ]
        self.mock_collection.find.return_value = timed_out_docs

        self.url_manager.check_timeout_urls()
        self.url_manager.error_bloom.add.assert_called_once_with("http://example1.com")
        self.mock_collection.delete_one.assert_called_once_with({"_id": 1})
        self.mock_collection.update_one.assert_called_once_with(
            {"_id": 2},
            {
                "$set": {"status": "to_crawl"},
                "$inc": {"crawling_count": 1}
            }
        )

    def test_manage_storage_save_to_local(self):
        self.mock_collection.count_documents.return_value = 7_000_000
        self.url_manager._save_to_local_storage = MagicMock()
        self.url_manager.manage_storage()
        self.url_manager._save_to_local_storage.assert_called_once()

    def test_manage_storage_load_from_local(self):
        self.mock_collection.count_documents.return_value = 500_000
        self.url_manager._load_from_local_storage = MagicMock()
        self.url_manager.manage_storage()
        self.url_manager._load_from_local_storage.assert_called_once()

    @patch("os.makedirs")
    def test_local_storage_creation(self, mock_makedirs):
        URLManager(self.mock_collection, local_storage_path="test_storage")
        mock_makedirs.assert_called_once_with("test_storage", exist_ok=True)

    @patch("os.listdir")
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data='[{"url": "http://example.com"}]')
    def test_load_from_local_storage(self, mock_open, mock_listdir):
        mock_listdir.return_value = ["file.json"]
        self.url_manager._load_from_local_storage()
        self.mock_collection.insert_many.assert_called_once_with([{"url": "http://example.com"}])
        mock_open.assert_called_once_with("local_storage/file.json", 'r')

    @patch("os.listdir")
    @patch("builtins.open", new_callable=unittest.mock.mock_open)
    @patch("os.remove")
    def test_save_to_local_storage(self, mock_remove, mock_open, mock_listdir):
        self.mock_collection.find.return_value = [{"url": "http://example1.com"}]
        self.url_manager._save_to_local_storage()
        mock_open.assert_called_once()
        mock_remove.assert_called_once()

if __name__ == "__main__":
    unittest.main()
