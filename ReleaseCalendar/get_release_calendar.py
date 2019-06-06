from datetime import datetime
import csv
import json
import os, re, argparse
import logging
import requests
import math

#import pandas as pd
#import csv
from datetime import timedelta, date, timedelta
from google.cloud import bigquery

bq_client = bigquery.Client()
dataset_ref = bq_client.dataset('sumo')

from google.cloud import storage
storage_client = storage.Client()
sumo_bucket = storage_client.get_bucket('moz-it-data-sumo')

logger = logging.getLogger(__name__)


base_url = "https://product-details.mozilla.org"


def reload_bq_table(uri, fn, table_name):

  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.autodetect = True

  job_config.skip_leading_rows = 1
  job_config.write_disposition = 'WRITE_TRUNCATE'

  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request
  print("Starting job {}".format(load_job.job_id))

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  print('Loaded {} rows into {}:{}.'.format(destination_table.num_rows, 'sumo', table_name))
  
  # move fn to processed folder
  blob = sumo_bucket.blob("release_calendar/" + fn)
  new_blob = sumo_bucket.rename_blob(blob, "release_calendar/processed/" + fn)
  print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))


def daterange(start_date, end_date):
  for n in range(int ((end_date - start_date).days)):
    yield start_date + timedelta(n)


def calc_durations(release_row, release_date, weeks_out):
  #print(datetime.strptime(release_date, '%Y-%m-%d'))
  release_datetime = datetime.strptime(release_date, '%Y-%m-%d')
  end_date = release_datetime + timedelta(weeks=weeks_out)
  #print(end_date)
  results = []

  for dt in daterange(release_datetime, end_date):
    num_days = (dt-release_datetime).days
    release_row_tmp = list(release_row)
    #print(release_row_tmp + [dt.strftime('%Y-%m-%d'), num_days+1, math.floor(num_days/7)+1])
    results.append(release_row_tmp + [dt.strftime('%Y-%m-%d'), num_days+1, math.floor(num_days/7)+1])
  print(results)
  return results


def get_release_calendar_row(release, row, weeks_out):

  release_datetime = datetime.strptime(row['date'], '%Y-%m-%d')
  end_date = release_datetime + timedelta(weeks=weeks_out)

  results = []

  for dt in daterange(release_datetime, end_date):
    num_days = (dt-release_datetime).days
    #release_row_tmp = list(release_row)
    #print(release_row_tmp + [dt.strftime('%Y-%m-%d'), num_days+1, math.floor(num_days/7)+1])
    #results.append(release_row_tmp + [dt.strftime('%Y-%m-%d'), num_days+1, math.floor(num_days/7)+1])
    results.append([release, row['product'], row['category'], row['build_number'], row['date'], row['version'],
          row.get('description',''), row.get('is_security_driven',False),
          dt.strftime('%Y-%m-%d'), num_days+1, math.floor(num_days/7)+1])

  #release_row = [release, row['product'], row['category'], row['build_number'], row['date'], row['version'],
  #        row.get('description',''), row.get('is_security_driven',False)]
  #return calc_durations(release_row, row['date'], 8)
  
  #print(results)
  return results
			

def update_release_calendar(url_version, product):
  
  start=datetime.now()
  
  weeks_out=8
  fileout = "/tmp/release_calendar.csv"
  
  url = base_url + "/" + url_version + "/" + product + ".json"
  print(url)

  # update entire table every time
  
  results = []
  response = requests.get(url)

  # need to get total_pages value and loop through to get all data &page=#
  fields = [["release","product","category","build_number","release_date","version","description","is_security_driven",
			"date_utc","day_num", "week_num"]]

  #results.append(fields)

  if response.status_code == 200:
    raw = response.json()
    for i in raw['releases']:
      #print(i)
      #print(raw['releases'][i]['category'])
      results.append(get_release_calendar_row(i,raw['releases'][i],weeks_out))
		
    logger.info('returning results')

    with open(fileout, "w") as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(fields)
      for it in results:
        writer.writerows(it)

    blob = sumo_bucket.blob("release_calendar/release_calendar.csv")
    blob.upload_from_filename(fileout)
    reload_bq_table("gs://moz-it-data-sumo/release_calendar/", "release_calendar.csv", 'release_calendar')  

  else:
    print('[!] HTTP {0} calling [{1}]'.format(response.status_code, api_url)) # 401 unauthorized
    #return None
      
  print(datetime.now()-start)


if __name__ == '__main__':
  url_version = "1.0" 
  product = "firefox"
  update_release_calendar(url_version, product)
