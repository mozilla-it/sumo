import datetime

import pandas as pd

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import Forbidden, NotFound

from Product_Insights.Classification.utils 
        import keywords_based_classifier
from Product_Insights.Classification.create_classification_table \
        import create_keywords_map
from Product_Insights.Classification.upload_keywords_map \
        import upload_keywords_map
from Product_Insights.Sentiment.utils \
        import gc_detect_language, gc_sentiment, discretize_sentiment
from Product_Insights.Twitter.create_twitter_tables \
        import create_twitter_sentiment

PROJECT_ID = 'marketing-1003'

INPUT_DATASET = 'sumo_views'
INPUT_TABLE = 'twitter_mentions_view'
OUTPUT_DATASET = 'analyse_and_tal'
OUTPUT_TABLE = 'twitter_sentiment'

OUTPUT_BUCKET = 'test-unique-bucket-name'

local_keywords_file = './Product_Insights/Classification/keywords_map.csv'

bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.get_bucket('test-unique-bucket-name')

def get_timeperiod():
  ''' Return the current time and last time data was previously saved with this scipt '''
  start_dt = datetime.datetime(2010, 5, 1).isoformat()
  end_dt = datetime.datetime.now().isoformat()
  
  qry_max_date = ("SELECT max(created_at) max_date FROM {0}.{1}")\
                   .format(OUTPUT_DATASET, OUTPUT_TABLE)

  query_job = bq_client.query(qry_max_date)
  try:
    max_date_result = query_job.to_dataframe() 
  except NotFound:
    create_twitter_sentiment(OUTPUT_DATASET, OUTPUT_TABLE)
    max_date_result = query_job.to_dataframe() 

  max_date = pd.to_datetime(max_date_result['max_date'].values[0])
  if max_date is not None:
    start_dt = max_date.isoformat()
  return(start_dt, end_dt)

def load_data(start_dt, end_dt, limit=None):
  '''Gets data from the input twitter table'''
  query = ('''SELECT * FROM `{0}.{1}.{2}` \
              WHERE `created_at` BETWEEN TIMESTAMP("{3}") AND TIMESTAMP("{4}") \
              ORDER BY `created_at` ASC''').\
              format(PROJECT_ID, INPUT_DATASET, INPUT_TABLE, 
                     start_dt, end_dt)
  if limit:
    query += ' LIMIT {}'.format(limit)
  try:
      df = bq_client.query(query).to_dataframe()
      return(df)
  except Exception as e:
      print(e)
      return(None)

def language_analysis(df):
  '''Guesses the language of each tweet'''
  d_lang = {}
  d_confidence = {}
  for i, row in df.iterrows():
      try:
          confidence, language = gc_detect_language(row.full_text)
          d_lang[row.id_str] = language
          d_confidence[row.id_str] = confidence
      except Forbidden as e:
          print(e)

  df[u'language'] = df['id_str'].map(d_lang)
  df[u'confidence'] = df['id_str'].map(d_confidence)
  return(df)

def filter_language(df, lang='en', lang_confidence=0.8):
  '''Filters out non-english tweets and removes lang columns'''
  df = df[(df.language == lang)&(df.confidence >= lang_confidence)]
  df = df.drop(['language', 'confidence'], axis=1)
  if df.empty:
    print('No data in dataframe after language filter')
  else:
    return(df)

def run_sentiment_analysis(df):
  '''Estimates the sentiment of each tweet'''
  sentiment_score = {}
  sentiment_magnitude = {}
  for i, row in df.iterrows():
      text = row.full_text
      score, magnitude = gc_sentiment(text)
      sentiment_score[row.id_str] = score
      sentiment_magnitude[row.id_str] = magnitude


  df[u'score'] = df['id_str'].map(sentiment_score)
  df[u'magnitude'] = df['id_str'].map(sentiment_magnitude)

  df[u'discrete_sentiment'] = df.apply(lambda x: \
                             discretize_sentiment(x['score'],x['magnitude']), axis=1)  

  return(df)

def get_keywords_map():
  '''Load the keywords map''' 
  table_name = 'keywords_map'
  query = 'SELECT * FROM `{}.{}.{}`'.format(PROJECT_ID, OUTPUT_DATASET, table_name)
  query_job = bq_client.query(query)
  try:
    keywords_map = query_job.to_dataframe() 
  except NotFound:
    create_keywords_map(OUTPUT_DATASET, table_name)
    upload_keywords_map(OUTPUT_BUCKET, local_keywords_file, table_name)
    query_job = bq_client.query(query)
    keywords_map = query_job.to_dataframe()
  return(keywords_map)

def get_topics(df, keywords_map):
  '''Determines the topic based on the keywords in keywords_map'''
  text_topic = {}
  
  #Detect topic based on keywords
  for i, row in df.iterrows():
    topics = keywords_based_classifier(row.full_text, keywords_map)

    #To enable writing our list of topics to a big query table
    #each topic has to be contained within a list of dicts
    #where each dicts key is topic and item is the topic at hand. 
    topics_list = [{'topic': ''}]
    for topic in topics:
      topics_list.append({'topic': topic})

    text_topic[row.id_str] = topics_list

  df[u'topics'] = df['id_str'].map(text_topic)
  return(df)


def update_bq_table(uri, fn, table_name):
  '''Saves data from a bq bucket to a table'''
  dataset_ref = bq_client.dataset(OUTPUT_DATASET)
  table_ref = dataset_ref.table(table_name)
  
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = "WRITE_APPEND"
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  job_config.autodetect = True
  
  orig_rows =  bq_client.get_table(table_ref).num_rows

  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request
  print("Starting job {}".format(load_job.job_id))

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  print('Loaded {} rows into {}:{}.'.format(destination_table.num_rows-orig_rows, 'sumo', table_name))
  blob = bucket.blob("twitter/" + fn)
  new_blob = bucket.rename_blob(blob, "twitter/processed/" + fn)
  
def save_results(df, start_dt, end_dt):
  '''Saves the dataframe to a gs bucket and a bq table'''
  fn = 'twitter_sentiment_' + start_dt[0:10] + "_to_" + end_dt[0:10] + '.json'
  
  df = df.set_index('id_str')

  df.to_json('/tmp/'+fn,  orient="records", lines=True,date_format='iso')

  blob = bucket.blob("twitter/" + fn)
  blob.upload_from_filename("/tmp/" + fn)

  update_bq_table("gs://{}/twitter/".format(bucket.name), fn, 'twitter_sentiment') 


def main():
  start_dt, end_dt = get_timeperiod()
  df = load_data(start_dt, end_dt, limit=300)
  if df is not None:
    df = language_analysis(df)
    df = filter_language(df)
    df = run_sentiment_analysis(df)
    keywords_map = get_keywords_map()
    df = get_topics(df, keywords_map)
    save_results(df, start_dt, end_dt)

if __name__ == '__main__':
  main()