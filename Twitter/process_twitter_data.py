import pandas as pd

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound, Conflict, Forbidden

from sumo.Sentiment.utils import gc_detect_language, gc_sentiment, discretize_sentiment

#df = pd.read_csv('../data/kitsune.csv')

PROJECT_ID = 'marketing-1003'
bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)
dataset_ref = bq_client.dataset('analyse_and_tal')

def load_data(limit=50):
  query = ('SELECT * \
           FROM `{0}.sumo_views.twitter_mentions_view` LIMIT {1}').format(PROJECT_ID, limit)
  try:
      df = bq_client.query(query).to_dataframe()
      return(df)
  except Exception as e:
      print(e)
      return(None)

def language_analysis(df):
  d_lang = {}
  d_confidence = {}
  for i, row in df.iterrows():
      try:
          confidence, language = gc_detect_language(row.full_text)
          d_lang[row.id_str] = language
          d_confidence[row.id_str] = confidence
      except Forbidden:
          print('Forbidden')

  df[u'language'] = df['id_str'].map(d_lang)
  df[u'confidence'] = df['id_str'].map(d_confidence)
  return(df)

def filter_language(df, lang='en', lang_confidence=0.8):
  df = df[(df.language == lang)&(df.confidence > lang_confidence)]
  return(df)

def run_sentiment_analysis(df):
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

def save_results(df):
  save_df = df[['id_str', 'score', 'magnitude', 'discrete_sentiment']]
  save_df = save_df.set_index('id_str')
  table_ref = dataset_ref.table('twitter_sentiment') 
  job = bq_client.load_table_from_dataframe(save_df, table_ref) 
  job.result()


def fill_driving_sentiment_table(df):
  driving_tweets = df.sort_values('score').iloc[0:10].sample(n=2).id_str.to_list()  
  save_df = df[df.id_str.isin(driving_tweets)]
  save_df = df[['id_str']]
  save_df['empty_filler'] = u''
  save_df = save_df.set_index('id_str')
  table_ref = dataset_ref.table('twitter_driving_sentiment') 
  job = bq_client.load_table_from_dataframe(save_df, table_ref) 
  job.result()


def main():
  df = load_data()
  df = language_analysis(df)
  df = filter_language(df, lang='en', lang_confidence=0.8)
  df = run_sentiment_analysis(df)
  save_results(df)
  fill_driving_sentiment_table(df)


# if __name__ == '__main__':
#   main()