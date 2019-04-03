from nltk.tokenize import word_tokenize
from collections import Counter
import re
from nltk.corpus import stopwords
import string
import pandas as pd

from datetime import datetime, timezone, timedelta

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
  content = [x.strip() for x in antivirus_words] 
 
def tokenize(s):
    return tokens_re.findall(s)
 
 
def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens


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
  #x=1
  #for key,value in count_all.most_common():
  #  print(str(x) + ": " + dt.strftime('%Y-%m-%d') + ": "+ key + ", " + str(value))
  #  x=x+1
  df = pd.DataFrame.from_dict(count_all, orient='index').reset_index()
  # add date zeroth date column
  df = df.rename(columns={'index':'tweet_word', 0:'tweet_freq'})
  df['tweet_dt'] = dt.strftime('%Y-%m-%d')
  print(df)
  df.to_csv('firefox_word_freq_' + dt.strftime('%Y%m%d') + '.csv', index=False)
  

if __name__ == '__main__':
  dt = datetime(2019, 3, 22)
  
  print(content)
  
  punctuation = list(string.punctuation)
  additional_ignore = ['n','firefox','mozilla','@firefox','@mozilla','-','’','…','—',':/']
  ignore_list = stopwords.words('english') + punctuation + ['rt', 'via'] + additional_ignore #+ antivirus_words
  # mozilla, firefox, all the antivirus ones (will do those separately so we dont duplicate
  # just add all word counts to list
  munge_data(dt, ignore_list) #'2019-03-13T00:00:00.000Z', '2019-03-14T00:00:00.000Z')
