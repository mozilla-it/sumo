from nltk.tokenize import word_tokenize
from collections import Counter
import re
from nltk.corpus import stopwords
import string
import pandas as pd
import gcsfs
import os

from datetime import datetime, timezone, timedelta

from google.cloud import bigquery
bq_client = bigquery.Client()
dataset_name = 'sumo'
dataset_ref = bq_client.dataset(dataset_name)

bucket = os.environ.get('BUCKET','moz-it-data-sumo')

from google.cloud import storage
storage_client = storage.Client()
sumo_bucket = storage_client.get_bucket(bucket)


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
##with open('gs://mz-it-data-sumo/ref/antivirus_keywords.txt') as f:
##  antivirus_words = f.readlines()
content = pd.read_csv('gs://{}/ref/antivirus_keywords.txt'.format(bucket))
#print(antivirus_words)
#remove whitespace characters like `\n` at the end of each line
#content = [x.strip() for x in antivirus_words] 


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
  blob = sumo_bucket.blob("twitter/" + fn)
  new_blob = sumo_bucket.rename_blob(blob, "twitter/processed/" + fn)
  print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))


def munge_data(dt, ignore_list):
  # aggregates tweets to daily
  #print(dt.strftime('%Y-%m-%dT%H:%M:%S0Z'))
  next_day = dt + timedelta(days=1)
  #print(next_day)
  start_dt = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z') #'2019-03-14T00:00:00.000Z'
  end_dt = next_day.strftime('%Y-%m-%dT%H:%M:%S.000Z') #'2019-03-15T00:00:00.000Z'

  sql_qry = """SELECT created_at, full_text FROM sumo.twitter_mentions where created_at >= timestamp('{0}') and created_at < timestamp('{1}')"""
  print(sql_qry.format(start_dt, end_dt))
  query_job = bq_client.query(sql_qry.format(start_dt, end_dt))

  results = query_job.result()  # Waits for job to complete.

  count_all = Counter()
  
  for row in results:
    #print("{} : {} views".format(row.url, row.view_count))
    #print(row.created_at) #row.full_text)
    #tokens = preprocess(row.full_text)
    #print(tokens)
    # Create a list with all the terms
    terms_all = [term for term in preprocess(row.full_text, True) if term not in ignore_list]
    # Update the counter
    count_all.update(terms_all)

  # Print the first 5 most frequent words
  #print(count_all) #.most_common(100))
  
  # update big query with list of frequent words? for given dt?
  df = pd.DataFrame.from_dict(count_all, orient='index').reset_index()
  if df.shape[0] >0 :
    # add date zeroth date column
    df = df.rename(columns={'index':'tweet_word', 0:'tweet_freq'})
    df['tweet_dt'] = dt.strftime('%Y-%m-%d')
    print(df)
  
    fn = 'firefox_word_freq_' + dt.strftime('%Y%m%d') + '.csv'
    df.to_csv('/tmp/' + fn, index=False)

    blob = sumo_bucket.blob("twitter/" + fn)
    blob.upload_from_filename("/tmp/" + fn)

    update_bq_table("gs://{}/twitter/".format(bucket), fn, 'twitter_word_frequencies')  

  
def main():
  #start_dt = datetime(2019, 3, 23) # inclusive
  end_dt = datetime.today().date() # exclusive, datetime.today().date()
  
  qry_max_dt = ("""SELECT max(tweet_dt) max_dt FROM {0} """).format(dataset_name + ".twitter_word_frequencies")
  query_job = bq_client.query(qry_max_dt)
  max_dt_result = query_job.to_dataframe() 
  try:
    start_dt = max_dt_result['max_dt'].values[0].date()
  except:
    start_dt = datetime(2019, 3, 23).date() # inclusive
  print(start_dt)
  
  #print(content)
  
  punctuation = list(string.punctuation)
  additional_ignore = ['n','firefox','mozilla','@firefox','@mozilla','-','’','…','—',':/']
  ignore_list = stopwords.words('english') + punctuation + ['rt', 'via'] + additional_ignore #+ antivirus_words

  # mozilla, firefox, all the antivirus ones (will do those separately so we dont duplicate
  # just add all word counts to list
  for dt in daterange(start_dt, end_dt):
    dt_str = dt.strftime("%Y-%m-%d")
    print(dt_str)

    munge_data(dt, ignore_list) 

if __name__ == '__main__':
  main()
