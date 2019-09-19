import unittest
import re

import pandas as pd

from Product_Insights.Classification.utils import get_regex_pattern, match_keywords, keywords_based_classifier

class GetRegexPatternTestCase(unittest.TestCase):
    """Tests for get_regex_pattern from Classification/utils.py"""

    def test_one_word_pattern(self): 
        r = get_regex_pattern(['first'])
        r_expected = re.compile(r'\bfirst\b ', re.IGNORECASE|re.VERBOSE)
        self.assertEqual(r, r_expected)

    def test_multiple_words_space_seperated_pattern(self): 
        r = get_regex_pattern(['first second'])
        r_expected = re.compile(r'\bfirst\ssecond\b ', re.IGNORECASE|re.VERBOSE)
        self.assertEqual(r, r_expected)

    def test_multiple_word_pattern(self): 
        r = get_regex_pattern(['first', 'second'])
        r_expected = re.compile(r'\bfirst\b |\bsecond\b ', re.IGNORECASE|re.VERBOSE)
        self.assertEqual(r, r_expected)

    def test_starting_with_pattern(self): 
        r = get_regex_pattern(['_first'])
        r_expected = re.compile(r'\b(first\w+)\b ', re.IGNORECASE|re.VERBOSE)
        self.assertEqual(r, r_expected)

    def test_ending_with_pattern(self): 
        r = get_regex_pattern(['first_'])
        r_expected = re.compile(r'\b(\w+first)\b ', re.IGNORECASE|re.VERBOSE)
        self.assertEqual(r, r_expected)

class MatchKeywordsTestCase(unittest.TestCase):
    """Tests for match_keywords from Classification/utils.py"""

    def test_empty_text_input(self):
        text = ''
        keyword = 'test'
        self.assertFalse(match_keywords(text, keyword))

    def test_one_word_match(self):
        text = 'simple test text'
        keyword = 'test'
        self.assertTrue(match_keywords(text, keyword))

    def test_one_word_nonmatch(self):
        text = 'simple test text'
        keyword = 'abc'
        self.assertFalse(match_keywords(text, keyword))

    def test_multiple_words_space_seperated_match(self):
        text = 'simple test text'
        keyword = 'test text'
        self.assertTrue(match_keywords(text, keyword))

    def test_multiple_words_space_seperated_nonmatch(self):
        text = 'simple test text'
        keyword = 'test abc'
        self.assertFalse(match_keywords(text, keyword))

    def test_multiple_word_match(self):
        text = 'simple test text'
        keyword = 'text, simple'
        self.assertTrue(match_keywords(text, keyword))

    def test_multiple_word_nonmatch(self):
        text = 'simple test text'
        keyword = 'text, abc'
        self.assertFalse(match_keywords(text, keyword))

    def test_starting_with_match(self):
        text = 'simple test text'
        keyword = '_tes'
        self.assertTrue(match_keywords(text, keyword))

    def test_starting_with_nonmatch(self):
        text = 'simple test text'
        keyword = '_abc'
        self.assertFalse(match_keywords(text, keyword))

    def test_ending_with_match(self):
        text = 'simple test text'
        keyword = 'est_'
        self.assertTrue(match_keywords(text, keyword))

    def test_ending_with_nonmatch(self):
        text = 'simple test text'
        keyword = 'abc_'
        self.assertFalse(match_keywords(text, keyword))

class KeywordsBasedClassifierTestCase(unittest.TestCase):
    """Tests for keywords_based_classifier from Classification/utils.py"""


    def setUp(self):
        self.keywords_map = pd.DataFrame([
                        {'topic': 'single word', 'keywords': 'abc'},
                        {'topic': 'multiple single words', 'keywords': 'abc'},
                        {'topic': 'multiple single words', 'keywords': 'def'},
                        {'topic': 'single phrase', 'keywords': 'abc def'},
                        {'topic': 'word combinations', 'keywords': 'abc, def'},
                        {'topic': 'word ending', 'keywords': 'abc_'},
                        {'topic': 'word begining', 'keywords': '_abc'},
                        ])

    def test_empty_text_input(self):
        text = ''
        result = keywords_based_classifier(text, self.keywords_map)
        expected_result = []
        self.assertEqual(result, expected_result)

    def test_no_matches(self):
        text = 'Test sentence'
        result = keywords_based_classifier(text, self.keywords_map)
        expected_result = []
        self.assertEqual(result, expected_result)

    def test_single_match(self):
        text = 'Test sentence def'
        result = keywords_based_classifier(text, self.keywords_map)
        expected_result = ['multiple single words']
        self.assertEqual(result, expected_result)

    def test_multiple_matches(self):
        text = 'Test sentence abc'
        result = keywords_based_classifier(text, self.keywords_map)
        self.assertIn('multiple single words', result)
        self.assertIn('single word', result)
        
    def test_single_phrase(self):
        text = 'Test sentence abc def'
        result = keywords_based_classifier(text, self.keywords_map)
        self.assertIn('single phrase', result)
        

    def test_word_combinations(self):
        text = 'Test sentence def abc'
        result = keywords_based_classifier(text, self.keywords_map)
        self.assertIn('word combinations', result)
        
    def test_word_combinations_and_phrase(self):
        text = 'Test sentence abc def'
        result = keywords_based_classifier(text, self.keywords_map)
        self.assertIn('word combinations', result)
        self.assertIn('single phrase', result)        

    def test_beginings(self):
        text = 'Test sentence abcword'
        result = keywords_based_classifier(text, self.keywords_map)
        self.assertIn('word begining', result)
        
    def test_word_endings(self): 
        text = 'Test sentence wordabc'
        result = keywords_based_classifier(text, self.keywords_map)
        self.assertIn('word ending', result)


if __name__ == '__main__':
    unittest.main()