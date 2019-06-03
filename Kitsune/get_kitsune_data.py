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

from google.cloud import bigquery
bq_client = bigquery.Client()

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
with open('./antivirus_keywords.txt') as f:
  antivirus_words = f.readlines()
  #remove whitespace characters like `\n` at the end of each line
  antivirus_content = [x.strip() for x in antivirus_words] 
 
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

def update_answers():
  start=datetime.now()
  url = "https://support.mozilla.org/api/2/answer"
  url_params= {'format': 'json', 'product': 'firefox', 'locale': 'en-US'} #,'page': '50000'} #, 'results_per_page': '500'} up to 56297?

  with open("./kitsune_answers_all.csv", "w", encoding='utf8') as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(Kitsune.get_answer_data(url, url_params))
  # need to update answers max 2019-03-05 14:19:54 UTC ; min 2010-01-05 10:24:54 UTC
  print(datetime.now()-start)
  
def update_questions():
  start=datetime.now()
  
  #https://support.mozilla.org/api/2/question/?_method=GET&locale=en-US&product=firefox&updated__lt=2010-05-10
  # total count 370727
  #updated__lt2011-01-01 32875
  #updated__lt=2010-12-31&updated__gt=2010-11-30

  url = "https://support.mozilla.org/api/2/question"
  url_params= {'format': 'json', 'product': 'firefox', 'locale': 'en-US', 'updated__lt': '2019-03-25', 'updated__gt': '2019-1-1'} #,'page': '12000'} #, 'page': '18463'}
  #url_params = {:format => "json",:product => "firefox", :locale => "en-US", :ordering => "+created", 'results_per_page': '500'}

  results = Kitsune.get_question_data(url, url_params)
  fields = ["question_id","question_content","created_date","creator_username",
			"is_solved","locale", "product", "title", "topic", "solution", "solved_by",
			"num_votes","num_votes_past_week", "last_answer", "metadata_array", "tags_array", "answers"]
  #df = pd.DataFrame.from_records(results, columns=fields )
  #  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  #df.to_csv(fName, index=False)
  with open("./kitsune_questions_20190101_to_20190325.csv", "w", encoding='utf8') as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(results)
      
  print(datetime.now()-start)
		
		
def analyze_word_freq(start_date, end_date):
  # munge questions by created date
  # note, does not account for case where question itself is updated (can that even happen?)

  df = pd.DataFrame()
  
  for dt in daterange(start_date, end_date):
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
  df.to_csv('kitsune_word_freq_' + start_date.strftime('%Y%m%d') + "_to_" + end_date.strftime('%Y%m%d') + '.csv', index=False)


def run():

  # use next to iterate to next page url
  #"https://support.mozilla.org/api/2/question/?_method=GET&locale=en-US&page=2&product=firefox"
  #"count","next","previous","results"
  # results per page limited to 19
  
  # ***we will maintain primary on id by deleting an older duplicate rows AFTER we perform table updates
  # ARGH annoying to do "except top 1" delete so do update on raw table, and have distinct id view

  # ALWAYS update answers first since questions depends on answers array
  update_answers()
  
  update_questions()
  
  analyze_word_freq(date(2019, 1, 1), date(2019, 3, 27))


if __name__ == '__main__':
	run()
