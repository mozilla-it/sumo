import unittest
from Product_Insights.Sentiment.utils import discretize_sentiment

class DiscretizeSentimentTestCase(unittest.TestCase):
    """Tests for discretize_sentiment from Sentiment/utils.py"""

    def test_low_magnitude_positive_score(self):
        score = 1
        magnitude = 0.1
        result = discretize_sentiment(score, magnitude, score_cutoff=0.2, magnitude_cutoff=0.5)
        self.assertEqual(result, 'neutral')

    def test_low_magnitude_negative_score(self):
        score = -1
        magnitude = 0
        result = discretize_sentiment(score, magnitude, score_cutoff=0.2, magnitude_cutoff=0.5)
        self.assertEqual(result, 'neutral')

    def test_high_magnitude_negative_score(self):
        score = -1
        magnitude = 1
        result = discretize_sentiment(score, magnitude, score_cutoff=0.2, magnitude_cutoff=0.5)
        self.assertEqual(result, 'negativ')

    def test_high_magnitude_positive_score(self):
        score = 1
        magnitude = 1
        result = discretize_sentiment(score, magnitude, score_cutoff=0.2, magnitude_cutoff=0.5)
        self.assertEqual(result, 'positive')

if __name__ == '__main__':
    unittest.main()