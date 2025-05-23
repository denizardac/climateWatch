import unittest
from data_processing.nlp_pipeline import get_sentiment, batch_sentiment

class TestNLPPipeline(unittest.TestCase):
    def test_get_sentiment(self):
        pos = get_sentiment("This is a great day!")
        neg = get_sentiment("This is a terrible disaster.")
        self.assertGreater(pos, 0)
        self.assertLess(neg, 0)

    def test_batch_sentiment(self):
        texts = ["I love climate action!", "I hate pollution.", None]
        results = batch_sentiment(texts)
        self.assertEqual(len(results), 3)
        self.assertIsInstance(results[0], float)
        self.assertIsInstance(results[1], float)
        self.assertIsNone(results[2])

if __name__ == "__main__":
    unittest.main() 