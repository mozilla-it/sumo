import datetime
import time

import pandas as pd

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from pytrends.request import TrendReq

from Product_Insights.GTrends.create_gtrends_tables \
        import create_tables

bq_client = bigquery.Client()
storage_client = storage.Client()

def check_last_update(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES, OUTPUT_TABLE_TS):
  qry_max_date = ("SELECT max(update_date) max_date FROM {0}.{1}")\
                   .format(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES)  
  query_job = bq_client.query(qry_max_date)
  try:
    max_date_result = query_job.to_dataframe() 
  except NotFound:
    create_tables(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES, OUTPUT_TABLE_TS)
    query_job = bq_client.query(qry_max_date)
    max_date_result = query_job.to_dataframe() 
  last_update = pd.to_datetime(max_date_result['max_date'].values[0])
  if last_update:
    last_update = last_update.date()

  return(last_update)

def get_collection_period(last_update):
  ''' Return the current time and last time data was previously saved with this scipt '''
  today = datetime.date.today()
  end_dt = today - datetime.timedelta(days=today.weekday()) 
  
  if last_update == end_dt:
    print('Already collected data for this week')
    return(None, None)
  else:
    start_dt = end_dt - datetime.timedelta(days=7)
    return(start_dt.isoformat(), end_dt.isoformat())

def get_gtrend(keyword, geo='', timeframe='now 7-d'):
    pytrends = TrendReq()
    pytrends.build_payload([keyword],  geo=geo, timeframe=timeframe)
    while True:
      try:
        results = pytrends.related_queries()
        rising_queries = results[keyword]['rising']
      except:
        time.sleep(60)
        continue
      break

    rising_queries_interest = {}
    if not rising_queries is None:
      for i, row in results[keyword]['rising'].iterrows():
        while True: 
          try:
            pytrends.build_payload([row.query],  geo=geo, timeframe=timeframe)
            rising_queries_interest[row.query] = pytrends.interest_over_time()
          except:
            time.sleep(60)
            continue
          break

    return(rising_queries, rising_queries_interest)

def get_data(start_dt, end_dt):
    geo_codes = ['', 'US', 'DE', 'IN', 'FR', 'RU', 'IT', 'BR', 'PL', 'CN', 'NL', 'JP', 'ES', 'ID']
    #Adding 'T00' ensures hourly resolution on result
    timeframe = start_dt +'T00' + ' ' + end_dt+'T00'
    data = {}
    for geo_code in geo_codes:        
        queries, q_interest = get_gtrend('Firefox', geo=geo_code, 
                                         timeframe=timeframe)
        data[geo_code] = (queries, q_interest)
        #Waiting 60 seconds due to rate-limit
        time.sleep(60)
    return(data)

def clean_queries(q, region, end_dt):
    q = q.rename({'value':'search_increase_pct'}, axis=1)
    q['region'] = region
    q['update_date'] = end_dt
    q['query_key_ts'] = end_dt + '#' + region +   '#' + q['query']
    return(q)    

def clean_related_queries(related_query, query, region, end_dt):
    related_query = related_query.reset_index()
    related_query.rename({'date':'timestamp', query:'relative_search_volume'},
                          axis=1, inplace=True)
    related_query.drop('isPartial', axis=1, inplace=True) 
    related_query['query'] = query
    related_query['query_key'] = end_dt + '#' + region +   '#' + query
    return(related_query)

def process_data(data, end_dt): 
    df_queries = pd.DataFrame()
    df_queries_ts = pd.DataFrame()

    for region in data.keys():
        q, related_qs = data[region]
        if not q is None:
          q = clean_queries(q, region, end_dt)
          df_queries = df_queries.append(q)

          for query in related_qs.keys():
              related_query = related_qs[query]
              related_query = clean_related_queries(related_query, query, region, end_dt)
              df_queries_ts = df_queries_ts.append(related_query)
    try:
      df_queries_ts = df_queries_ts[['query', 'query_key', 'relative_search_volume', 'timestamp']]
    except KeyError:
      df_queries_ts = None
    return(df_queries, df_queries_ts)

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
  blob = bucket.blob("gtrends/" + fn)
  new_blob = bucket.rename_blob(blob, "gtrends/processed/" + fn)

def save_results(OUTPUT_DATASET, OUTPUT_TABLE, OUTPUT_BUCKET, df, start_dt, end_dt):
  '''Saves the dataframe to a gs bucket and a bq table'''

  bucket = storage_client.get_bucket(OUTPUT_BUCKET)

  fn = OUTPUT_TABLE +'_' + start_dt + "_to_" + end_dt + '.json'
  
  df = df.set_index('query')

  df.to_json('/tmp/'+fn,  orient="records", lines=True,date_format='iso')
  
  blob = bucket.blob("gtrends/" + fn)
  blob.upload_from_filename("/tmp/" + fn)

  dataset_ref = bq_client.dataset(OUTPUT_DATASET)
  table_ref = dataset_ref.table(OUTPUT_TABLE)

  update_bq_table("gs://{}/gtrends/".format(bucket.name), fn, table_ref) 
  move_blob_to_processed(bucket,fn)


def collect_data(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES, OUTPUT_TABLE_TS, OUTPUT_BUCKET):
    last_update = check_last_update(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES, OUTPUT_TABLE_TS)
    start_dt, end_dt = get_collection_period(last_update)
    if start_dt: 
        data = get_data(start_dt, end_dt)
        df_queries, df_queries_ts = process_data(data, end_dt)
        save_results(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES, OUTPUT_BUCKET, df_queries, start_dt, end_dt) 
        if not df_queries_ts.empty:
          save_results(OUTPUT_DATASET, OUTPUT_TABLE_TS, OUTPUT_BUCKET, df_queries_ts, start_dt, end_dt) 
