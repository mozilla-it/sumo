from datetime import datetime
import json
import requests
import csv
import os, re, argparse
import Kitsune

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


def get_data(api_url_base, params):
	api_url = '{0}?_method=GET'.format(api_url_base)
	results = []
	response = requests.get(api_url, params=params)
			   
	if response.status_code == 200:
		raw = response.json()
		for i in raw['results']:
		  print(i)
		  #results.append(get_survey_data_row(i))
		#results.append(raw)
		return results

	else:
		print('[!] HTTP {0} calling [{1}]'.format(response.status_code, api_url)) # 401 unauthorized
		return None

def run():

  start=datetime.now()
  url = "https://support.mozilla.org/api/2/question"
  url_params= {'format': 'json', 'product': 'firefox', 'locale': 'en-US'} 

  with open("./kitsune_questions.csv", "w", encoding='utf8') as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(Kitsune.get_question_data(url, url_params))
      
  print(datetime.now()-start)

  start=datetime.now()
  url = "https://support.mozilla.org/api/2/answer"
  url_params= {'format': 'json', 'product': 'firefox', 'locale': 'en-US'} #,'page': '50000'} #, 'results_per_page': '500'} up to 56297?

  with open("./kitsune_answers.csv", "w", encoding='utf8') as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(Kitsune.get_answer_data(url, url_params))

  print(datetime.now()-start)

if __name__ == '__main__':
	run()
