import pandas as pd

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import Forbidden

from sumo.Product_Insights.Sentiment.utils \
        import gc_detect_language, gc_sentiment, discretize_sentiment

PROJECT_ID = 'marketing-1003'
bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)
dataset_ref = bq_client.dataset('analyse_and_tal')

def load_data(limit=50, dataset='sumo_views', table='twitter_mentions_view'):
  query = ('SELECT * \
           FROM `{0}.{1}.{2}` LIMIT {3}').format(PROJECT_ID, dataset, table, limit)
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


def get_topics(df):
  df['topic'] = 'na'
  return(df)

def save_results(df):
  save_df = df[['id_str', 'score', 'magnitude', 'discrete_sentiment']]
  save_df = save_df.set_index('id_str')
  table_ref = dataset_ref.table('twitter_sentiment') 
  job = bq_client.load_table_from_dataframe(save_df, table_ref) 
  job.result()


def main():
  df = load_data()
  df = language_analysis(df)
  df = filter_language(df, lang='en', lang_confidence=0.8)
  df = run_sentiment_analysis(df)
  df = get_topics(df)
  save_results(df)


if __name__ == '__main__':
  main()