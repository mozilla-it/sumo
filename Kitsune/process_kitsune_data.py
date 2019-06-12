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

def load_data(limit=500):
  query = ('SELECT question_id, question_content, title, topic \
           FROM `{0}.sumo_views.kitsune_questions_view` LIMIT {1}').format(PROJECT_ID, limit)
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
          confidence, language = gc_detect_language(row.title + row.question_content)
          d_lang[row.question_id] = language
          d_confidence[row.question_id] = confidence
      except Forbidden:
          print('Forbidden')

  df[u'language'] = df['question_id'].map(d_lang)
  df[u'confidence'] = df['question_id'].map(d_confidence)
  return(df)

def filter_language(df, lang='en', lang_confidence=0.8):
  df = df[(df.language == lang)&(df.confidence > lang_confidence)]
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


def save_results(df):
  save_df = df[['question_id', 'score', 'magnitude', 'discrete_sentiment', 'topic']]
  save_df = save_df.set_index('question_id')
  table_ref = dataset_ref.table('kitsune_sentiment') 
  job = bq_client.load_table_from_dataframe(save_df, table_ref) 
  job.result()


def fill_driving_sentiment_table(df):
  driving_questions = df.sort_values('score').iloc[0:10].sample(n=2).question_id.to_list()  
  save_df = df[df.question_id.isin(driving_questions)]
  save_df = df[['question_id', 'topic']]
  save_df = save_df.set_index('question_id')
  table_ref = dataset_ref.table('kitsune_driving_sentiment') 
  job = bq_client.load_table_from_dataframe(save_df, table_ref) 
  job.result()


# Save to db


def main():
  df = load_data()
  df = language_analysis(df)
  df = filter_language(df, lang='en', lang_confidence=0.8)
  df = run_sentiment_analysis(df)
  save_results(df)
  fill_driving_sentiment_table(df)


if __name__ == '__main__':
  df = main()