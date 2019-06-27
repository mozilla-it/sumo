import datetime

import pandas as pd

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound, Forbidden

from Product_Insights.Sentiment.utils \
       import gc_detect_language, gc_sentiment, discretize_sentiment
from Product_Insights.Kitsune.create_kitsune_tables \
        import create_kitsune_sentiment

bq_client = bigquery.Client()
storage_client = storage.Client()

def get_timeperiod(OUTPUT_DATASET, OUTPUT_TABLE):
  ''' Return the current time and last time data was previously saved with this scipt '''
  start_dt = datetime.datetime(2010, 5, 1).isoformat()
  end_dt = datetime.datetime.now().isoformat()
  
  qry_max_date = ("SELECT max(created_date) max_date FROM {0}.{1}")\
                   .format(OUTPUT_DATASET, OUTPUT_TABLE)

  query_job = bq_client.query(qry_max_date)
  try:
    max_date_result = query_job.to_dataframe() 
  except NotFound:
    create_kitsune_sentiment(OUTPUT_DATASET, OUTPUT_TABLE)
    query_job = bq_client.query(qry_max_date)
    max_date_result = query_job.to_dataframe() 

  max_date = pd.to_datetime(max_date_result['max_date'].values[0])
  if max_date is not None:
    start_dt = max_date.isoformat()
  return(start_dt, end_dt)


def load_data(INPUT_DATASET, INPUT_TABLE,start_dt, end_dt, limit=None):
  '''Gets data from the input table'''
  query = ('''SELECT * FROM `{0}.{1}` \
              WHERE `created_date` BETWEEN TIMESTAMP("{2}") AND TIMESTAMP("{3}") \
              ORDER BY `created_date` ASC''').\
              format(INPUT_DATASET, INPUT_TABLE, start_dt, end_dt)
  if limit:
    query += ' LIMIT {}'.format(limit)
  try:
      df = bq_client.query(query).to_dataframe()
      df = df.drop(['solution', 'last_answer'], axis=1)
      return(df)
  except Exception as e:
      print(e)
      return(None)

def language_analysis(df):
  d_lang = {}
  d_confidence = {}
  for i, row in df.iterrows():
      try:
          confidence, language = gc_detect_language(row.title + row.question_content)
          d_lang[row.question_id] = language
          d_confidence[row.question_id] = confidence
      except Forbidden as e:
          print(e)

  df[u'language'] = df['question_id'].map(d_lang)
  df[u'confidence'] = df['question_id'].map(d_confidence)
  return(df)

def filter_language(df, lang='en', lang_confidence=0.8):
  df = df[(df.language == lang)&(df.confidence > lang_confidence)]
  df = df.drop(['language', 'confidence'], axis=1)
  if df.empty:
    print('No data in dataframe after language filter')
  else:
    return(df)


def run_sentiment_analysis(df):
  sentiment_score = {}
  sentiment_magnitude = {}
  for i, row in df.iterrows():
      text = row.title + row.question_content
      score, magnitude = gc_sentiment(text, type='HTML')
      sentiment_score[row.question_id] = score
      sentiment_magnitude[row.question_id] = magnitude


  df[u'score'] = df['question_id'].map(sentiment_score)
  df[u'magnitude'] = df['question_id'].map(sentiment_magnitude)

  df[u'discrete_sentiment'] = df.apply(lambda x: \
                             discretize_sentiment(x['score'],x['magnitude']), axis=1)  

  return(df)




def update_bq_table(uri, fn, table_ref):
  '''Saves data from a bq bucket to a table'''

  
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = "WRITE_APPEND"
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  job_config.autodetect = True
  
  orig_rows =  bq_client.get_table(table_ref).num_rows

  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request
  print("Starting job {}".format(load_job.job_id))

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  print('Loaded {} rows into {}:{}.'.format(destination_table.num_rows-orig_rows, 'sumo', table_ref.table_id))

  
def move_blob_to_processed(bucket,fn):
  blob = bucket.blob("kitsune/" + fn)
  new_blob = bucket.rename_blob(blob, "kitsune/processed/" + fn)

def save_results(OUTPUT_DATASET, OUTPUT_TABLE, OUTPUT_BUCKET, df, start_dt, end_dt):
  '''Saves the dataframe to a gs bucket and a bq table'''

  bucket = storage_client.get_bucket(OUTPUT_BUCKET)

  fn = 'kitsune_sentiment_' + start_dt[0:10] + "_to_" + end_dt[0:10] + '.json'
  
  df = df.set_index('question_id')

  df.apply(lambda x: x.dropna(), axis=1).to_json('/tmp/'+fn,  orient="records", lines=True,date_format='iso')
  
  blob = bucket.blob("kitsune/" + fn)
  blob.upload_from_filename("/tmp/" + fn)

  dataset_ref = bq_client.dataset(OUTPUT_DATASET)
  table_ref = dataset_ref.table(OUTPUT_TABLE)

  update_bq_table("gs://{}/kitsune/".format(bucket.name), fn, table_ref) 
  move_blob_to_processed(bucket,fn)

def get_unprocessed_data(OUTPUT_DATASET, OUTPUT_TABLE, INPUT_DATASET, INPUT_TABLE):
  start_dt, end_dt = get_timeperiod(OUTPUT_DATASET, OUTPUT_TABLE)
  df = load_data(INPUT_DATASET, INPUT_TABLE, start_dt, end_dt, limit=500)
  #df = load_data(INPUT_DATASET, INPUT_TABLE, start_dt, end_dt)
  return(df, start_dt, end_dt)

def get_sentiment(df):
  df = language_analysis(df)
  df = filter_language(df)
  df = run_sentiment_analysis(df)
  return(df)

def process_data(INPUT_DATASET, INPUT_TABLE, OUTPUT_DATASET, OUTPUT_TABLE, OUTPUT_BUCKET):
  df, start_dt, end_dt = get_unprocessed_data(OUTPUT_DATASET, OUTPUT_TABLE, INPUT_DATASET, INPUT_TABLE)
  if df is not None:
    df = get_sentiment(df)
    save_results(OUTPUT_DATASET, OUTPUT_TABLE, OUTPUT_BUCKET, df, start_dt, end_dt)

