import unittest
import datetime
import time

import pandas as pd
from pandas.testing import assert_frame_equal

from Product_Insights.GTrends.collect_gtrends_data import check_last_update, get_collection_period, save_results, get_gtrend
from Product_Insights.GTrends.create_gtrends_tables import create_tables, create_gtrends_timeseries
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound, Conflict

class CheckLastUpdateTestCase(unittest.TestCase):
    """Tests for check_last_update from Gtrends/collect_gtrends_data.py"""

    def setUp(self):
        self.OUTPUT_DATASET = 'test_dataset_for_test_process_gtrends_data'
        self.OUTPUT_TABLE_QUERIES = 'test_table_for_test_process_gtrends_data'
        self.OUTPUT_TABLE_TS = 'test_table_ts_for_test_process_gtrends_data'

        self.bq_client = bigquery.Client()

        try:
            self.bq_client.create_dataset(self.OUTPUT_DATASET)
        except Conflict:
            pass

    def test_wrong_output_dataset(self):
        self.OUTPUT_DATASET = 'somenonsensedataset'

        with self.assertRaises(NotFound):
            check_last_update(self.OUTPUT_DATASET, self.OUTPUT_TABLE_QUERIES, self.OUTPUT_TABLE_TS)

    def test_new_table_returns_none(self):
        last_update = check_last_update(self.OUTPUT_DATASET, self.OUTPUT_TABLE_QUERIES, self.OUTPUT_TABLE_TS)
        self.assertIsNone(last_update)        

    def test_new_output_table_gets_made(self):
        last_update = check_last_update(self.OUTPUT_DATASET, self.OUTPUT_TABLE_QUERIES, self.OUTPUT_TABLE_TS)
        bq_tables = [i.table_id for i in self.bq_client.list_tables(self.OUTPUT_DATASET)]

        self.assertIn(self.OUTPUT_TABLE_QUERIES, bq_tables)
        self.assertIn(self.OUTPUT_TABLE_TS, bq_tables)


    def tearDown(self):
        # delete dataset
        try:
            tables = self.bq_client.list_tables(self.OUTPUT_DATASET)
            for table in tables:
                self.bq_client.delete_table('{}.{}'.format(self.OUTPUT_DATASET, table.table_id))
            self.bq_client.delete_dataset(self.OUTPUT_DATASET)
        except NotFound:
            pass

class GetCollectionPeriodTestCase(unittest.TestCase):
    """Tests for get_collection_period from Gtrends/collect_gtrends_data.py"""

    def test_input_none(self):
        today = datetime.date.today()
        end_dt_expected = today - datetime.timedelta(days=today.weekday())
        start_dt_expected = end_dt_expected - datetime.timedelta(days=7)

        start_dt, end_dt = get_collection_period(None)

        self.assertEqual(start_dt, start_dt_expected.isoformat())
        self.assertEqual(end_dt, end_dt_expected.isoformat())

    def test_input_not_none(self):
        today = datetime.date.today() 
        last_update = today - datetime.timedelta(days=7)
        start_dt, end_dt = get_collection_period(last_update)
        
        end_dt_expected = today - datetime.timedelta(days=today.weekday())
        start_dt_expected = end_dt_expected - datetime.timedelta(days=7)

        self.assertEqual(start_dt, start_dt_expected.isoformat())
        self.assertEqual(end_dt, end_dt_expected.isoformat())


class GetTrendTestCase(unittest.TestCase):
    """Tests for get_gtrend from GTrends/collect_gtrends_data.py"""

    def test_gtrend_response(self):
        keyword = 'Hello'

        rising_queries, rising_queries_interest = get_gtrend(keyword, geo='', timeframe='now 7-d')

        self.assertIsInstance(rising_queries, pd.DataFrame)

        self.assertIn('query', rising_queries.columns)
        self.assertIn('value', rising_queries.columns)

        self.assertIsInstance(rising_queries_interest, dict)
        
class SaveResultsTestCase(unittest.TestCase):
    """Tests for save_results from GTrends/collect_gtrends_data.py"""

    def setUp(self):
        self.OUTPUT_BUCKET = 'test_bucket_for_test_process_gtrends_data'
        self.OUTPUT_DATASET = 'test_dataset_for_test_process_gtrends_data'
        self.OUTPUT_TABLE_QUERIES = 'test_table_for_test_process_gtrends_data'
        self.OUTPUT_TABLE_TS = 'test_table_ts_for_test_process_gtrends_data'
        self.start_dt = datetime.datetime.today().date().isoformat()
        self.end_dt = datetime.datetime.today().date().isoformat()

        self.df = pd.DataFrame([{'query_key': "thisisaquerykey",
                                 'timestamp': datetime.datetime.now(), 
                                 'original_query': 'test text', 
                                 'relative_search_volume':10, 
                                 }])

        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()
        
        #Make sure we have a test dataset we can write to
        try:
            self.bq_client.create_dataset(self.OUTPUT_DATASET)
        except Conflict:
            pass

        create_gtrends_timeseries(self.OUTPUT_DATASET, self.OUTPUT_TABLE_TS)

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
        
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE_TS, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        fn = self.OUTPUT_TABLE_TS + '_' + self.start_dt + "_to_" + self.end_dt + '.json'

        self.assertTrue(self.bucket.blob("gtrends/processed/" + fn).exists())

    def test_load_into_empty_table(self):
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE_TS, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        dataset_ref = self.bq_client.dataset(self.OUTPUT_DATASET)
        table_ref = dataset_ref.table(self.OUTPUT_TABLE_TS)

        orig_rows =  self.bq_client.get_table(table_ref).num_rows
        self.assertIs(orig_rows, 1)


    def test_load_into_nonempty_table(self):
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE_TS, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        save_results(self.OUTPUT_DATASET, self.OUTPUT_TABLE_TS, self.OUTPUT_BUCKET, 
                     self.df, self.start_dt, self.end_dt)      
        
        dataset_ref = self.bq_client.dataset(self.OUTPUT_DATASET)
        table_ref = dataset_ref.table(self.OUTPUT_TABLE_TS)

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