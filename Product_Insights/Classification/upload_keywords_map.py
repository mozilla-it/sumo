from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import Conflict

PROJECT_ID = 'marketing-1003'
bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)
dataset_ref = bq_client.dataset('analyse_and_tal')

filename = './Product_Insights/Classification/keywords_map.csv'
bucket_name = 'classification-test'

def update_bq_table(uri, fn, table_name):

  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.field_delimiter = '\t'
  
  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  
try:
  bucket = storage_client.create_bucket(bucket_name)
except Conflict:
  bucket = storage_client.get_bucket(bucket_name)

blob = bucket.blob('keywords_map.csv')
blob.upload_from_filename(filename)


fn = 'keywords_map.csv'
update_bq_table("gs://{}/".format(bucket_name), fn, 'keywords_map')

