import unittest
from data_processing.scraping import get_article_text, batch_scrape

class TestScraping(unittest.TestCase):
    def test_get_article_text(self):
        url = "https://www.example.com/"
        text = get_article_text(url)
        self.assertIsInstance(text, (str, type(None)))

    def test_batch_scrape(self):
        urls = ["https://www.example.com/", "https://www.iana.org/domains/example"]
        results = batch_scrape(urls, sleep_time=0)
        self.assertEqual(len(results), 2)
        for item in results:
            self.assertIn("url", item)
            self.assertIn("text", item)

if __name__ == "__main__":
    unittest.main() 