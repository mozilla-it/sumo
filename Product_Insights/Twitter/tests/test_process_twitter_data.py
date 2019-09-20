import unittest
import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from google.cloud import bigquery, storage
from Product_Insights.Twitter.process_twitter_data import get_timeperiod, load_data, language_analysis, filter_language, run_sentiment_analysis, save_results, get_keywords_map, determine_topics
from Product_Insights.Twitter.create_twitter_tables import create_twitter_sentiment
from google.cloud.exceptions import NotFound, Conflict
from Product_Insights.Classification.create_classification_table \
        import create_keywords_map
from Product_Insights.Classification.upload_keywords_map \
        import upload_keywords_map

class GetTimeperiodTestCase(unittest.TestCase):
    """Tests for get_timeperiod from Twitter/process_twitter_data.py"""

    def setUp(self):
        self.OUTPUT_DATASET = 'test_dataset_for_test_process_twitter_data'
        self.OUTPUT_TABLE = 'test_table_for_test_process_twitter_data'
        
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
        create_twitter_sentiment(self.OUTPUT_DATASET, self.OUTPUT_TABLE)

        test_data = [{'id_str': "1",
                      'created_at': datetime.datetime.now(), 
                      'full_text': 'test text', 
                      'user_id':10, 
                      'in_reply_to_status_id_str': None,     
                      'score': None, 
                      'magnitude': None, 
                      'discrete_sentiment': None,
                      'topics': ['test']
                      }]

        test_table = self.bq_client.get_table('{}.{}'.format(self.OUTPUT_DATASET, self.OUTPUT_TABLE))
        self.bq_client.insert_rows(test_table, test_data)
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
    """Tests for load_data from Twitter/process_twitter_data.py"""

    def setUp(self):
        self.INPUT_DATASET = 'test_dataset_for_test_process_twitter_data'
        self.INPUT_TABLE = 'test_table_for_test_process_twitter_data'
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
    """Tests for language_analysis from Twitter/process_twitter_data.py"""

    
    def setUp(self):
        self.df = pd.DataFrame([{'id_str': "1", 'full_text': 'some english tweet'},
                                {'id_str': "2", 'full_text': ''}, 
                                {'id_str': "3", 'full_text': 'Ikke engelsk spoergsmaal'}])

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
    """Tests for filter_language from Twitter/process_twitter_data.py"""

    
    def setUp(self):
        self.df = pd.DataFrame([{'id_str': '1', 'language': 'en', 'confidence': 1},
                                {'id_str': '2', 'language': 'en', 'confidence': 0.1}, 
                                {'id_str': '3', 'language': 'und', 'confidence': 1}, 
                                {'id_str': '4', 'language': 'und', 'confidence': 0.1}])

    def test_output_format(self):
        self.assertIn('id_str',filter_language(self.df).columns)
        self.assertNotIn('language',filter_language(self.df).columns)
        self.assertNotIn('confidence',filter_language(self.df).columns)

    def test_normal_output_content(self): 
        results = filter_language(self.df).id_str.unique()
        self.assertIn('1', results)
        self.assertNotIn('2', results)
        self.assertNotIn('3', results)
        self.assertNotIn('4', results)

    def test_no_questions_fulfill_requirements(self): 
        results = filter_language(self.df, lang='abc', lang_confidence = 2)
        self.assertTrue(results.empty)

class RunSentimentAnalysisTestCase(unittest.TestCase):
    """Tests for run_sentiment_analysis from Twitter/process_twitter_data.py"""
    
    def setUp(self):
        self.df = pd.DataFrame([{'id_str': '1', 'full_text': 'Happy tweet'},
                                {'id_str': '2', 'full_text': 'Neutral tweet'}, 
                                {'id_str': '3', 'full_text': 'Horrible tweet'}])

    def test_output_format(self):
        self.assertIn('score',run_sentiment_analysis(self.df).columns)
        self.assertIn('magnitude',run_sentiment_analysis(self.df).columns)
        self.assertIn('discrete_sentiment',run_sentiment_analysis(self.df).columns)

    def test_output_content(self):
        results = run_sentiment_analysis(self.df)
        self.assertEqual('positive', results.discrete_sentiment.iloc[0])
        self.assertEqual('neutral', results.discrete_sentiment.iloc[1])
        self.assertEqual('negativ', results.discrete_sentiment.iloc[2])


class GetKeywordsMapTestCase(unittest.TestCase):
    """Tests for get_keywords_map from Twitter/process_twitter_data.py"""

    def setUp(self):
        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()
        self.OUTPUT_DATASET = 'test_dataset_for_test_process_twitter_data'
        self.OUTPUT_BUCKET = 'bucket_for_test_process_twitter_data'
        self.local_keywords_file = "Product_Insights/Twitter/tests/data/keywords_map.tsv"

        try:
            self.bq_client.create_dataset(self.OUTPUT_DATASET)
        except Conflict:
            pass

        try:
            blobs = self.storage_client.bucket(self.OUTPUT_BUCKET).list_blobs()
            for blob in blobs:
                blob.delete()
            self.storage_client.bucket(self.OUTPUT_BUCKET).delete()
        except NotFound:
            pass

        self.storage_client.create_bucket(self.OUTPUT_BUCKET)
        self.bucket = self.storage_client.bucket(self.OUTPUT_BUCKET)


    def test_correct_output_if_table_not_found(self): 
        try:
            self.bq_client.delete_table('{}.{}'.format(self.OUTPUT_DATASET, 'keywords_map'))
        except NotFound:
            pass

        keywords_map = get_keywords_map(self.OUTPUT_DATASET, self.OUTPUT_BUCKET, self.local_keywords_file)

        keywords_map_expected = pd.read_csv(self.local_keywords_file, sep='\t')

        assert_frame_equal(keywords_map, keywords_map_expected, check_dtype=False)
    
    def test_correct_output_if_table_is_found(self): 
        try:
            self.bq_client.delete_table('{}.{}'.format(self.OUTPUT_DATASET, 'keywords_map'))
        except NotFound:
            pass

        create_keywords_map(self.OUTPUT_DATASET, 'keywords_map')
        upload_keywords_map(self.OUTPUT_BUCKET, self.local_keywords_file, self.OUTPUT_DATASET, 'keywords_map')



        keywords_map = get_keywords_map(self.OUTPUT_DATASET, self.OUTPUT_BUCKET, self.local_keywords_file)

        keywords_map_expected = pd.read_csv(self.local_keywords_file, sep='\t')

        assert_frame_equal(keywords_map, keywords_map_expected, check_dtype=False)



    def tearDown(self):    
        # delete dataset
        try:
            tables = self.bq_client.list_tables(self.OUTPUT_DATASET)
            for table in tables:
                self.bq_client.delete_table('{}.{}'.format(self.OUTPUT_DATASET, table.table_id))
            self.bq_client.delete_dataset(self.OUTPUT_DATASET)
        except NotFound:
            pass

        # delete bucket
        try:
            blobs = self.storage_client.bucket(self.OUTPUT_BUCKET).list_blobs()
            for blob in blobs:
                blob.delete()
            self.storage_client.bucket(self.OUTPUT_BUCKET).delete()

        except NotFound:
            pass


class DetermineTopicsTestCase(unittest.TestCase):
    """Tests for determine_topics from Twitter/process_twitter_data.py"""
    def setUp(self):

        self.df = pd.DataFrame([{'id_str': "1",
                                   'created_at': datetime.datetime.now(), 
                                   'full_text': 'test keywords 1', 
                                   'user_id':10, 
                                   'in_reply_to_status_id_str': "10",     
                                   'score': 1.0, 
                                   'magnitude': 1.0, 
                                   'discrete_sentiment': 'positive'
                                   },
                                   {'id_str': "2",
                                   'created_at': datetime.datetime.now(), 
                                   'full_text': 'test keywords 2', 
                                   'user_id':10, 
                                   'in_reply_to_status_id_str': "10",     
                                   'score': 1.0, 
                                   'magnitude': 1.0, 
                                   'discrete_sentiment': 'positive'
                                   }])

        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()
        self.OUTPUT_DATASET = 'test_dataset_for_test_process_twitter_data'
        self.OUTPUT_BUCKET = 'bucket_for_test_process_twitter_data'
        self.local_keywords_file = "Product_Insights/Twitter/tests/data/keywords_map.tsv"


        try:
            self.bq_client.create_dataset(self.OUTPUT_DATASET)
        except Conflict:
            pass

        try:
            blobs = self.storage_client.bucket(self.OUTPUT_BUCKET).list_blobs()
            for blob in blobs:
                blob.delete()
            self.storage_client.bucket(self.OUTPUT_BUCKET).delete()

        except NotFound:
            pass

        self.storage_client.create_bucket(self.OUTPUT_BUCKET)
        self.bucket = self.storage_client.bucket(self.OUTPUT_BUCKET)
        self.keywords_map = get_keywords_map(self.OUTPUT_DATASET, self.OUTPUT_BUCKET, self.local_keywords_file)

    def test_df_columns(self):
        df_results = determine_topics(self.df, self.keywords_map)
        self.assertIn('topics', df_results.columns)

        self.assertTrue(1, len(df_results.topics.iloc[1]))

    def test_topic_detected(self):
        df_results = determine_topics(self.df, self.keywords_map)
        self.assertTrue(2, len(df_results.topics.iloc[0]))


    def test_topic_not_detected(self):
        df_results = determine_topics(self.df, self.keywords_map)
        self.assertTrue(1, len(df_results.topics.iloc[1]))

    def tearDown(self):    
        # delete dataset
        try:
            tables = self.bq_client.list_tables(self.OUTPUT_DATASET)
            for table in tables:
                self.bq_client.delete_table('{}.{}'.format(self.OUTPUT_DATASET, table.table_id))
            self.bq_client.delete_dataset(self.OUTPUT_DATASET)
        except NotFound:
            pass

        # delete bucket
        try:
            blobs = self.storage_client.bucket(self.OUTPUT_BUCKET).list_blobs()
            for blob in blobs:
                blob.delete()
            self.storage_client.bucket(self.OUTPUT_BUCKET).delete()

        except NotFound:
            pass

class SaveResultsTestCase(unittest.TestCase):
    """Tests for save_results from Twitter/process_twitter_data.py"""


    def setUp(self):
        self.OUTPUT_BUCKET = 'test_bucket_for_test_process_twitter_data'
        self.OUTPUT_DATASET = 'test_dataset_for_test_process_twitter_data'
        self.OUTPUT_TABLE = 'test_table_for_test_process_twitter_data'
        self.start_dt = "2010-05-01T00:00:00"
        self.end_dt = "2019-10-01T00:00:00"

        self.df = pd.DataFrame([{'id_str': "1",
                                 'created_at': datetime.datetime.now(), 
                                 'full_text': 'test text', 
                                 'user_id':10, 
                                 'in_reply_to_status_id_str': "10",     
                                 'topics': None,
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

        create_twitter_sentiment(self.OUTPUT_DATASET, self.OUTPUT_TABLE)

        #Make sure we have an empty test bucket we can write to
        try:
            blobs = self.storage_client.bucket(self.OUTPUT_BUCKET).list_blobs()
            for blob in blobs:
                blob.delete()
            self.storage_client.bucket(self.OUTPUT_BUCKET).delete()
        except NotFound:
            pass

        self.storage_client.create_bucket(self.OUTPUT_BUCKET)
        self.bucket = self.storage_client.bucket(self.OUTPUT_BUCKET)

    def test_if_blob_in_processed_bucket(self):
        
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        fn = 'twitter_sentiment_' + self.start_dt[0:10] + "_to_" + self.end_dt[0:10] + '.json'

        self.assertTrue(self.bucket.blob("twitter/processed/" + fn).exists())

    def test_load_into_empty_table(self):
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        dataset_ref = self.bq_client.dataset(self.OUTPUT_DATASET)
        table_ref = dataset_ref.table(self.OUTPUT_TABLE)

        orig_rows =  self.bq_client.get_table(table_ref).num_rows
        self.assertIs(orig_rows, 1)


    def test_load_into_nonempty_table(self):
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        dataset_ref = self.bq_client.dataset(self.OUTPUT_DATASET)
        table_ref = dataset_ref.table(self.OUTPUT_TABLE)

        orig_rows =  self.bq_client.get_table(table_ref).num_rows
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