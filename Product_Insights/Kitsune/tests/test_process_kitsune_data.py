import unittest
import datetime

import pandas as pd

from google.cloud import bigquery, storage
from Product_Insights.Kitsune.process_kitsune_data import get_timeperiod, load_data, language_analysis, filter_language, run_sentiment_analysis, save_results
from Product_Insights.Kitsune.create_kitsune_tables import create_kitsune_sentiment
from google.cloud.exceptions import NotFound, Conflict

class GetTimeperiodTestCase(unittest.TestCase):
    """Tests for get_timeperiod from Kitsune/process_kitsune_data.py"""

    def setUp(self):
        self.OUTPUT_DATASET = 'test_dataset_for_test_process_kitsune_data'
        self.OUTPUT_TABLE = 'test_table_for_test_process_kitsune_data'
        
        self.bq_client = bigquery.Client()
        try:
            self.bq_client.create_dataset(self.OUTPUT_DATASET)
        except Conflict:
            pass

    def test_wrong_output_dataset(self):
        self.OUTPUT_DATASET = 'somenonsensedataset'

        with self.assertRaises(NotFound):
            get_timeperiod(self.OUTPUT_DATASET, self.OUTPUT_TABLE)

    def test_new_output_table_gets_made(self):
        start_dt, end_dt = get_timeperiod(self.OUTPUT_DATASET, self.OUTPUT_TABLE)
        bq_tables = [i.table_id for i in self.bq_client.list_tables(self.OUTPUT_DATASET)]

        self.assertIn(self.OUTPUT_TABLE, bq_tables)

    def test_new_output_table_correct_output(self):
        start_dt, end_dt = get_timeperiod(self.OUTPUT_DATASET, self.OUTPUT_TABLE)
        end_dt = end_dt[0:10]

        start_dt_expected = '2010-05-01T00:00:00'
        end_dt_expected = datetime.datetime.now().isoformat()[0:10]

        self.assertEqual(end_dt, end_dt_expected)
        self.assertEqual(start_dt, start_dt_expected)

    def test_existing_table_correct_output(self):
        create_kitsune_sentiment(self.OUTPUT_DATASET, self.OUTPUT_TABLE)

        test_data = [{'question_id': 1,
                      'question_content': 'test', 
                      'created_date': datetime.datetime.now(), 
                      'creator_username': 'test', 
                      'updated': datetime.datetime.now(), 
                      'updated_by': 'test', 
                      'is_solved': True, 
                      'locale': 'test', 
                      'product': 'test', 
                      'title': 'test', 
                      'topic': 'test', 
                      'solved_by': 'test', 
                      'num_votes': 1,
                      'num_votes_past_week': 1,
                      'metadata_array': 'test', 
                      'tags_array': 'test', 
                      'answers': 'test',     
                      'score': None, 
                      'magnitude': None, 
                      'discrete_sentiment': None
                      }]

        test_table = self.bq_client.get_table('{}.{}'.format(self.OUTPUT_DATASET, self.OUTPUT_TABLE))
        self.bq_client.insert_rows(test_table, test_data)

        start_dt, end_dt = get_timeperiod(self.OUTPUT_DATASET, self.OUTPUT_TABLE)
        start_dt = start_dt[0:10]
        end_dt = end_dt[0:10]
        
        start_dt_expected = datetime.datetime.now().isoformat()[0:10]
        end_dt_expected = datetime.datetime.now().isoformat()[0:10]

        self.assertEqual(end_dt, end_dt_expected)
        self.assertEqual(start_dt, start_dt_expected)

    def tearDown(self):
        # delete dataset
        try:
            tables = self.bq_client.list_tables(self.OUTPUT_DATASET)
            for table in tables:
                self.bq_client.delete_table('{}.{}'.format(self.OUTPUT_DATASET, table.table_id))
            self.bq_client.delete_dataset(self.OUTPUT_DATASET)
        except NotFound:
            pass


class LoadDataTestCase(unittest.TestCase):
    """Tests for load_data from Kitsune/process_kitsune_data.py"""

    def setUp(self):
        self.INPUT_DATASET = 'test_dataset_for_test_process_kitsune_data'
        self.INPUT_TABLE = 'test_table_for_test_process_kitsune_data'
        self.start_dt = "2010-05-01T00:00:00"
        self.end_dt = "2019-10-01T00:00:00"

    def test_wrong_input_dataset(self):
        self.INPUT_DATASET = 'somenonsensedataset'
        self.assertIsNone(load_data(self.INPUT_DATASET, self.INPUT_TABLE,
                             self.start_dt, self.end_dt, limit=None))

    def test_wrong_input_table(self):
        self.INPUT_TABLE = 'somenonsensedataset'
        self.assertIsNone(load_data(self.INPUT_DATASET, self.INPUT_TABLE,
                             self.start_dt, self.end_dt, limit=None))

class LanguageAnalysisTestCase(unittest.TestCase):
    """Tests for language_analysis from Kitsune/process_kitsune_data.py"""

    
    def setUp(self):
        self.df = pd.DataFrame([{'question_id': 1, 'title': 'some english', 'question_content': 'question'},
                                {'question_id': 2, 'title': '', 'question_content': ''}, 
                                {'question_id': 3, 'title': 'Ikke engelsk', 'question_content': 'spoergsmaal'}])

    def test_output_format(self):
        self.assertIn('language',language_analysis(self.df).columns)
        self.assertIn('confidence',language_analysis(self.df).columns)

    def test_english_language(self):
        results = language_analysis(self.df)
        self.assertEqual('en', results.language.iloc[0])

    def test_missing_text(self):
        results = language_analysis(self.df)
        self.assertEqual('und', results.language.iloc[1])

    def test_nonenglish_text(self):
        results = language_analysis(self.df)
        self.assertNotEqual('en', results.language.iloc[2])

class FilterLanguageTestCase(unittest.TestCase):
    """Tests for filter_language from Kitsune/process_kitsune_data.py"""

    
    def setUp(self):
        self.df = pd.DataFrame([{'question_id': 1, 'language': 'en', 'confidence': 1},
                                {'question_id': 2, 'language': 'en', 'confidence': 0.1}, 
                                {'question_id': 3, 'language': 'und', 'confidence': 1}, 
                                {'question_id': 4, 'language': 'und', 'confidence': 0.1}])

    def test_output_format(self):
        self.assertIn('question_id',filter_language(self.df).columns)
        self.assertNotIn('language',filter_language(self.df).columns)
        self.assertNotIn('confidence',filter_language(self.df).columns)

    def test_normal_output_content(self): 
        results = filter_language(self.df).question_id.unique()
        self.assertIn(1, results)
        self.assertNotIn(2, results)
        self.assertNotIn(3, results)
        self.assertNotIn(4, results)

    def test_no_questions_fulfill_requirements(self): 
        results = filter_language(self.df, lang='abc', lang_confidence = 2)
        self.assertIsNone(results)

class RunSentimentAnalysisTestCase(unittest.TestCase):
    """Tests for run_sentiment_analysis from Kitsune/process_kitsune_data.py"""
    
    def setUp(self):
        self.df = pd.DataFrame([{'question_id': 1, 'title': 'Happy title', 'question_content': 'Happy question'},
                                {'question_id': 2, 'title': 'Neutral title', 'question_content': 'Neutral question'}, 
                                {'question_id': 3, 'title': 'Horrible title', 'question_content': 'Horrible question'}])

    def test_output_format(self):
        self.assertIn('score',run_sentiment_analysis(self.df).columns)
        self.assertIn('magnitude',run_sentiment_analysis(self.df).columns)
        self.assertIn('discrete_sentiment',run_sentiment_analysis(self.df).columns)

    def test_output_content(self):
        results = run_sentiment_analysis(self.df)
        self.assertEqual('positive', results.discrete_sentiment.iloc[0])
        self.assertEqual('neutral', results.discrete_sentiment.iloc[1])
        self.assertEqual('negativ', results.discrete_sentiment.iloc[2])




class SaveResultsTestCase(unittest.TestCase):
    """Tests for save_results from Kitsune/process_kitsune_data.py"""


    def setUp(self):
        self.OUTPUT_BUCKET = 'test_bucket_for_test_process_kitsune_data'
        self.OUTPUT_DATASET = 'test_dataset_for_test_process_kitsune_data'
        self.OUTPUT_TABLE = 'test_table_for_test_process_kitsune_data'
        self.start_dt = "2010-05-01T00:00:00"
        self.end_dt = "2019-10-01T00:00:00"

        self.df = pd.DataFrame([{'question_id': 1,
                                 'question_content': 'test', 
                                 'created_date': datetime.datetime.now(), 
                                 'creator_username': 'test', 
                                 'updated': datetime.datetime.now(), 
                                 'updated_by': 'test', 
                                 'is_solved': True, 
                                 'locale': 'test', 
                                 'product': 'test', 
                                 'title': 'test', 
                                 'topic': 'test', 
                                 'solved_by': 'test', 
                                 'num_votes': 1,
                                 'num_votes_past_week': 1,
                                 'metadata_array': 'test', 
                                 'tags_array': 'test', 
                                 'answers': 'test',     
                                 'score': 1.0, 
                                 'magnitude': 1.0, 
                                 'discrete_sentiment': 'positive'
                                 }])

        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()
        
        #Make sure we have a test dataset we can write to
        try:
            self.bq_client.create_dataset(self.OUTPUT_DATASET)
        except Conflict:
            pass
        create_kitsune_sentiment(self.OUTPUT_DATASET, self.OUTPUT_TABLE)

        self.storage_client.create_bucket(self.OUTPUT_BUCKET)
        self.bucket = self.storage_client.bucket(self.OUTPUT_BUCKET)

    def test_if_blob_in_processed_bucket(self):
        
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        fn = 'kitsune_sentiment_' + self.start_dt[0:10] + "_to_" + self.end_dt[0:10] + '.json'

        self.assertTrue(self.bucket.blob("kitsune/processed/" + fn).exists())

    def test_load_into_empty_table(self):
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        dataset_ref = self.bq_client.dataset(self.OUTPUT_DATASET)
        table_ref = dataset_ref.table(self.OUTPUT_TABLE)

        orig_rows =  self.bq_client.get_table(table_ref).num_rows
        print(orig_rows)
        self.assertIs(orig_rows, 1)


    def test_load_into_nonempty_table(self):
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        dataset_ref = self.bq_client.dataset(self.OUTPUT_DATASET)
        table_ref = dataset_ref.table(self.OUTPUT_TABLE)

        orig_rows =  self.bq_client.get_table(table_ref).num_rows
        print(orig_rows)
        self.assertIs(orig_rows, 2)

    def tearDown(self):

        try:
            tables = self.bq_client.list_tables(self.OUTPUT_DATASET)
            for table in tables:
                self.bq_client.delete_table('{}.{}'.format(self.OUTPUT_DATASET, table.table_id))
            self.bq_client.delete_dataset(self.OUTPUT_DATASET)
        except NotFound:
            pass

        #Make sure we have an empty test bucket we can write to
        try:
            blobs = self.storage_client.bucket(self.OUTPUT_BUCKET).list_blobs()
            for blob in blobs:
                blob.delete()
            self.storage_client.bucket(self.OUTPUT_BUCKET).delete()
        except NotFound:
            pass


if __name__ == '__main__':
    unittest.main()