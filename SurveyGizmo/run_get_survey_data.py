import SurveyGizmo #includes json lib
from datetime import datetime
import csv
import json
import base64
import os, re, argparse

import google.cloud.logging
# Instantiates a client
client = google.cloud.logging.Client()
# Connects the logger to the root logging handler
client.setup_logging()

import logging

logging.info('start logging')
logger = logging.getLogger(__name__)

from google.cloud import storage
storage_client = storage.Client()

def main():
  parser = argparse.ArgumentParser(description="SUMO Survey Gizmo main arguments")
  parser.add_argument('--outdir', nargs='?', const='.', type=str, help='file output directory')
  args = parser.parse_args()

  outdir = args.outdir

  print(outdir)

  start=datetime.now()

  api_token = os.environ['SUMO_SURVEYGIZMO_TOKEN'] 
  api_secret_key = os.environ['SUMO_SURVEYGIZMO_KEY'] 
    
  survey_id = '4669267'
  results_per_page = '500' # takes about 30min to download all pages
  api_url_base = 'https://restapi.surveygizmo.com/v5/survey/' + survey_id + '/surveyresponse.json'

  params = {'resultsperpage': results_per_page, 'api_token': api_token, 'api_secret_key': api_secret_key, 'page': str(1)}

  with open("/tmp/csat_results.csv", "w", encoding='utf8') as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(SurveyGizmo.get_survey_data(api_url_base, params))
  
  bucket = storage_client.get_bucket('moz-it-data-sumo')
  blob = bucket.blob('surveygizmo/csat_results.csv')
  blob.upload_from_filename("/tmp/csat_results.csv")

  SurveyGizmo.update_bq_table("gs://moz-it-data-sumo/surveygizmo/csat_results.csv", 'sumo', 'surveygizmo')

  print(datetime.now()-start)

if __name__ == '__main__':
  main()
