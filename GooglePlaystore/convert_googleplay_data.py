from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

import codecs
import csv

# Imports the Google Cloud client library
from google.cloud import storage
# Instantiates a client
storage_client = storage.Client()

from google.cloud import bigquery
bq_client = bigquery.Client()

dataset_ref = bq_client.dataset('sumo')

playstore_storage_bucket = 'pubsite_prod_rev_04753778179066947806'


def convert_file_for_BQ_old(fn):
  with codecs.open(fn, 'rU', 'UTF-16') as infile:
    with open(fn + '.utf8', 'wb') as outfile:
      for line in infile:
        outfile.write(line.encode('utf8'))
        

def convert_file_for_BQ_new(fn):

  with codecs.open(fn, 'rU', 'UTF-16') as infile:
    with open(fn + '.utf8', 'wb') as outfile:
      for line in infile:
        # if Reviewer Language is ja or zh-Hans, make sure Review Text is surrounded by double quotes
        l = line.strip().split(',')
        # replace any commas in Reviews Title and Text and Developer Repla[y Text
        #if any(commas in l[11] for commas in [",","、","，"]):
        if any(commas in l[11] for commas in [","]):
          print(l[11])
          l[11] = l[11].replace("、"," ").replace("，", " ").replace(",", " ")
          print(l[11])
          
        l[10] = l[10].replace("、"," ").replace("，", " ").replace(",", " ")
        l[11] = l[11].replace("、"," ").replace("，", " ").replace(",", " ")
        #l[14] = l[14].replace("、"," ").replace("，", " ").replace(",", " ")
        
        line_new = l[0] + "," + l[1] + "," + l[2] + "," + l[3] + "," + \
                   l[4] + "," + l[5] + "," + l[6] + "," + l[7] + "," + \
                   l[8] + "," + l[9] + "," + l[10] + "," + l[11] + "," + \
                   l[12] + "," + l[13] + "," + l[14] + "," + l[15] + "\n"
                   #l[12].replace("\"","") + "," + l[13].replace("\"","") + "," + l[14] + "," + l[15] + "\n"
        #print(line_new.encode('utf8'))
        outfile.write(line_new.encode('utf8'))


def download_playstore_files(bucket_name, source_blob_name, destination_file_name):
  """Downloads a blob from the bucket."""
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(source_blob_name)

  blob.download_to_filename(destination_file_name) # overwrites any existing monthly file

  print('Blob {} downloaded to {}.'.format(source_blob_name, destination_file_name))


def update_bq_table(fn, table_name):

  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.skip_leading_rows = 1
  job_config.autodetect = True

  with open(fn, 'rb') as source_file:
    job = bq_client.load_table_from_file(source_file,table_ref,location='US',job_config=job_config)  # API request

  job.result()  # Waits for table load to complete.
  # need to add some error check/handling if now all rows loaded
  print('Loaded {} rows into {}:{}.'.format(job.output_rows, 'sumo', table_name))


def playstore_to_bq(dt):
  type = 'reviews'
  product = 'org.mozilla.firefox'
  yyyymm = dt.strftime("%Y%m")
  #from_dt = date(2019, 1, 1) # or get todays date's month? # or get 
  # generalize this to list of products
  fn = type + "_" + product + "_" + yyyymm + ".csv" #'reviews_org.mozilla.firefox_201812.csv'
  report_to_download = type + '/' + fn
  print(report_to_download)
  
  # Monthly file appended daily based on field "Review Last Update Date and Time"

  try:
    # get latest "Review Last Update Date and Time" for package from BQ 
    # this assumes no two reviews have the same timestamp - for lack of a better primary key (I think this is fine)
    
    # read playstore file
    download_playstore_files(playstore_storage_bucket, report_to_download, fn)
    convert_file_for_BQ(fn)
    
    # delete dates in table where Review_Last_Update_Date_and_Time in yyyymm ('yyyy-mm-dd')
    sql_qry = """DELETE FROM sumo.googleplaystore_reviews WHERE Review_Last_Update_Date_and_Time>='{0}' and Review_Last_Update_Date_and_Time<='{1}'"""
    first_dt = datetime.strptime(yyyymm+'01', '%Y%m%d')
    last_dt = first_dt + relativedelta(day=1, months=+1, days=-1)
    print(sql_qry.format(first_dt.strftime("%Y-%m-%d"), last_dt.strftime("%Y-%m-%d")))
    query_job = bq_client.query(sql_qry.format(first_dt.strftime("%Y-%m-%d"), last_dt.strftime("%Y-%m-%d")))
    result = query_job.result()  # Waits for job to complete.
    
    update_bq_table(fn + '.utf8', 'googleplaystore_reviews')
    
  except Exception as e:
    # Just exit if any error
    print("BQ return error : " + str(e))
  
            
if __name__ == '__main__':
  dt_today_utc = datetime.utcnow()
  
  #if dt_today_utc.day==1:
  #  playstore_to_bq(dt_today_utc - timedelta(days=1))
  # grrr there is lag - on 4/1 (PST), only getting 3/30 in march file
    
  #playstore_to_bq(dt_today_utc)
  convert_file_for_BQ_old('reviews_org.mozilla.firefox_2018.csv')
  #update_bq_table('reviews_reviews_org.mozilla.firefox_201806.csv.utf8', 'googleplaystore_reviews_tmp')
  
  # logs seem to update 12-7PM UTC, so ideally run after that
  # logs accumulated daily. to be safe, for first of month (UTC), first run on last day of previous month if exists
  # Only update from most recent
  # if we need to "fix" intermediary, then either manual or delete table and refresh files and reload EVERYTHING
  
  #gsutil -m rsync -r gs://pubsite_prod_rev_04753778179066947806/reviews gs://moz-it-data-sumo/googleplaystore
