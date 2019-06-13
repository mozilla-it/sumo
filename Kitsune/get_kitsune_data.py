from datetime import datetime, date, timedelta
import json
import requests
import csv
import os, re, argparse
import Kitsune

from nltk.tokenize import word_tokenize
from collections import Counter
from nltk.corpus import stopwords

import pandas as pd
from datetime import datetime, timedelta, date, timedelta

antivirus_content = ['ahnlab','avast','avg','avira','bitdefender','clamwin','comodo','defender',
                     'drweb','f-secure','f-prot','g data','forticlient','immunet','kaspersky','mcafee',
                     'nod32','norton','panda','quick heal','secureanywhere','sophos','titanium',
                     'trustport','webroot','windows defender']

import google.cloud.logging
# Instantiates a client
client = google.cloud.logging.Client()
# Connects the logger to the root logging handler
client.setup_logging()

import logging

logging.info('start logging')
logger = logging.getLogger(__name__)

bucket = os.environ.get('BUCKET','moz-it-data-sumo')

from google.cloud import storage
storage_client = storage.Client()
sumo_bucket = storage_client.get_bucket(bucket)

from google.cloud import bigquery
bq_client = bigquery.Client()
dataset_name = 'sumo'
dataset_ref = bq_client.dataset(dataset_name)


emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""
 
regex_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
 
    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
]
    
tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)

# read antivirus_keywords.txt to list
#with open('gs://oz-it-data-sumo/ref/antivirus_keywords.txt') as f:
#  antivirus_words = f.readlines()
#  #remove whitespace characters like `\n` at the end of each line
#  antivirus_content = [x.strip() for x in antivirus_words] 
 
def tokenize(s):
    return tokens_re.findall(s)
    

def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens
    

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def update_bq_table(uri, fn, table_name):

  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = "WRITE_APPEND"
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.skip_leading_rows = 1
  job_config.autodetect = True
  
  orig_rows =  bq_client.get_table(table_ref).num_rows

  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request
  print("Starting job {}".format(load_job.job_id))

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  print('Loaded {} rows into {}:{}.'.format(destination_table.num_rows-orig_rows, 'sumo', table_name))
  
  # move fn to processed folder
  blob = sumo_bucket.blob("kitsune/" + fn)
  new_blob = sumo_bucket.rename_blob(blob, "kitsune/processed/" + fn)
  print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))


def update_answers():
  start=datetime.now()
  
  start_dt = datetime(2010, 5, 1).date()
  end_dt = datetime.today().date() # + timedelta(1)
  
  qry_max_date = ("""SELECT max(updated) max_date FROM {0} """).format(dataset_name + ".kitsune_answers_raw")
  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() 
  max_date = pd.to_datetime(max_date_result['max_date'].values[0])
  if max_date is not None:
    start_dt = max_date.date() #.astype(datetime).strftime('%Y-%m-%d')
  print(start_dt)

  assert start_dt <= end_dt,"Start Date >= End Date, no update needed."
  
  fn = "kitsune_answers_" + start_dt.strftime("%Y%m%d") + "_to_" + end_dt.strftime("%Y%m%d") + ".csv"
  print(fn)
  
  url = "https://support.mozilla.org/api/2/answer"
  #url_params= {'format': 'json', 'product': 'firefox', 'locale': 'en-US'} #,'page': '50000'} #, 'results_per_page': '500'} up to 56297?
  url_params= {'format': 'json', 'product': 'firefox', 'locale': 'en-US', 'updated__gt': start_dt.strftime("%Y-%m-%d")} #,'page': '12000'} #, 'page': '18463'}

  with open("/tmp/" + fn, "w", encoding='utf8') as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(Kitsune.get_answer_data(url, url_params))
  
  blob = sumo_bucket.blob("kitsune/" + fn)
  blob.upload_from_filename("/tmp/" + fn)

  update_bq_table("gs://{}/kitsune/".format(bucket), fn, 'kitsune_answers_raw')  
  
  print(datetime.now()-start)
  
def update_questions():
  start=datetime.now()
  
  #https://support.mozilla.org/api/2/question/?_method=GET&locale=en-US&product=firefox&updated__lt=2010-05-10
  # total count 370727
  #updated__lt2011-01-01 32875
  #updated__lt=2010-12-31&updated__gt=2010-11-30

  start_dt = datetime(2010, 5, 1).date()
  end_dt = datetime.today().date() # + timedelta(1)
  
  qry_max_date = ("""SELECT max(updated) max_date FROM {0} """).format(dataset_name + ".kitsune_questions_raw")
  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() 
  max_date = pd.to_datetime(max_date_result['max_date'].values[0])
  if max_date is not None:
    start_dt = max_date.date() #.astype(datetime).strftime('%Y-%m-%d')
  print(start_dt)

  assert start_dt <= end_dt,"Start Date >= End Date, no update needed."
  
  fn = "kitsune_questions_" + start_dt.strftime("%Y%m%d") + "_to_" + end_dt.strftime("%Y%m%d") + ".csv"
  print(fn)

  url = "https://support.mozilla.org/api/2/question"
  url_params= {'format': 'json', 'product': 'firefox', 'locale': 'en-US', 'updated__gt': start_dt.strftime("%Y-%m-%d")} #,'page': '12000'} #, 'page': '18463'}
  #url_params= {'format': 'json', 'product': 'firefox', 'locale': 'en-US', 'updated__lt': '2019-03-25', 'updated__gt': '2019-1-1'} #,'page': '12000'} #, 'page': '18463'}
  #url_params = {:format => "json",:product => "firefox", :locale => "en-US", :ordering => "+created", 'results_per_page': '500'}

  results = Kitsune.get_question_data(url, url_params)
  fields = ["question_id","question_content","created_date","creator_username",
			"is_solved","locale", "product", "title", "topic", "solution", "solved_by",
			"num_votes","num_votes_past_week", "last_answer", "metadata_array", "tags_array", "answers"]
  #df = pd.DataFrame.from_records(results, columns=fields )
  #  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  #df.to_csv(fName, index=False)
  with open("/tmp/" + fn, "w", encoding='utf8') as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(results)

  blob = sumo_bucket.blob("kitsune/" + fn)
  blob.upload_from_filename("/tmp/" + fn)

  update_bq_table("gs://{}/kitsune/".format(bucket), fn, 'kitsune_questions_raw')  
  
  print(datetime.now()-start)
		
		
def analyze_word_freq():
  # munge questions by created date
  # note, does not account for case where question itself is updated (can that even happen?)

  start_dt = datetime(2010, 5, 1).date()
  end_dt = datetime.today().date() # + timedelta(1)
  
  qry_max_date = ("""SELECT max(kitsune_dt) max_date FROM {0} """).format(dataset_name + ".kitsune_word_frequencies")
  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() 
  max_date = pd.to_datetime(max_date_result['max_date'].values[0])
  #start_dt = max_date.date() #.astype(datetime).strftime('%Y-%m-%d')
  #max_date = max_date_result['max_date'].values[0]
  if max_date is not None:
    start_dt = max_date
  print(start_dt)

  assert start_dt <= end_dt,"Start Date >= End Date, no update needed."
  
  fn = 'kitsune_word_freq_' + start_dt.strftime('%Y%m%d') + "_to_" + end_dt.strftime('%Y%m%d') + '.csv'
  print(fn)

  df = pd.DataFrame()
  
  for dt in daterange(start_dt, end_dt):
    #print single_date.strftime("%Y-%m-%d")
    next_day = dt + timedelta(days=1)
    #print(next_day)
    dt_str = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z') #'2019-03-14T00:00:00.000Z'
    next_dt_str = next_day.strftime('%Y-%m-%dT%H:%M:%S.000Z') #'2019-03-15T00:00:00.000Z'

    sql_qry = """SELECT created_date, question_content, title, metadata_array FROM sumo.kitsune_questions where created_date >= timestamp('{0}') and created_date < timestamp('{1}')"""
    print(sql_qry.format(dt_str, next_dt_str))
    query_job = bq_client.query(sql_qry.format(dt_str, next_dt_str))

    results = query_job.result()  # Waits for job to complete.

    count_all = Counter()
  
    # count +1 per question_id (even if word is mentioned multiple times)
    for row in results:
      #print(row.question_content + " " + row.title)
      # count question freq for each av word in content field
      #terms_av = [term for term in preprocess(row.question_content + " " + row.title, True) if term in antivirus_content]
      terms_av = [term for term in antivirus_content if term in preprocess(row.question_content + " " + row.title, True)]
      # count question freq where metadata contains "linux" , "OS X"
      #terms_os = [term for term in preprocess(row.metadata_array, True) if term in ['os x', 'linux']]
      terms_os = [term for term in ['os x', 'linux'] if term in preprocess(row.metadata_array, True) ]
      #print(terms_av)
      # Update the counter
      count_all.update(terms_av)
      count_all.update(terms_os)

    #print(dt)
    #print(count_all)

    df_tmp = pd.DataFrame.from_dict(count_all, orient='index').reset_index()
    # add date zeroth date column
    df_tmp = df_tmp.rename(columns={'index':'kitsune_word', 0:'kitsune_freq'})
    df_tmp['kitsune_dt'] = dt.strftime('%Y-%m-%d')
    df = df.append(df_tmp)
    
  df['kitsune_freq'] = df['kitsune_freq'].astype(int)
  print(df)
  df.to_csv("/tmp/" + fn, index=False)
  
  blob = sumo_bucket.blob("kitsune/" + fn)
  blob.upload_from_filename("/tmp/" + fn)

  update_bq_table("gs://{}/kitsune/".format(bucket), fn, 'kitsune_word_frequencies')  


def main():

  # use next to iterate to next page url
  #"https://support.mozilla.org/api/2/question/?_method=GET&locale=en-US&page=2&product=firefox"
  #"count","next","previous","results"
  # results per page limited to 19
  
  # ***we will maintain primary on id by deleting an older duplicate rows AFTER we perform table updates
  # ARGH annoying to do "except top 1" delete so do update on raw table, and have distinct id view

  # ALWAYS update answers first since questions depends on answers array

  print("bucket value: " + bucket)
  
  update_answers()
  
  update_questions()
  
  analyze_word_freq()


if __name__ == '__main__':
	main()
