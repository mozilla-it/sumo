from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import Conflict

bq_client = bigquery.Client()
storage_client = storage.Client()

bucket_name = 'classification-test'
    
def upload_keywords_map(bucket_name, local_file, dataset_name, table_name):
  try:
    bucket = storage_client.create_bucket(bucket_name)
  except Conflict:
    bucket = storage_client.get_bucket(bucket_name)

  filename = local_file.split('/')[-1]
  blob = bucket.blob(filename)
  blob.upload_from_filename(local_file)
  update_bq_table("gs://{}/".format(bucket_name), filename, dataset_name, table_name)


def update_bq_table(uri, fn, dataset_name, table_name):

  dataset_ref = bq_client.dataset(dataset_name)
  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = "WRITE_TRUNCATE"
  job_config.skip_leading_rows = 1
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.field_delimiter = '\t'
  
  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  

