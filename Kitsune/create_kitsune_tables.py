# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('analyse_and_tal')


#"answer_id","question_id","answer_content","created_date","creator_username",
#"updated","updated_by", "is_spam", "num_helpful_votes", "num_unhelpful_votes"
def create_kitsune_answers():
  schema = [
    bigquery.SchemaField('answer_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('question_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('answer_content', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('created_date', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('creator_username', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('updated', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('updated_by', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('is_spam', 'BOOLEAN', mode='NULLABLE'),
    bigquery.SchemaField('num_helpful_votes', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('num_unhelpful_votes', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('kitsune_answers_raw')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'kitsune_answers_raw'


#"question_id","question_content","created_date","creator_username","is_solved",
#"locale","product","title","topic","solution","solved_by","num_votes","num_votes_past_week",
#"last_answer","metadata_array","tags_array","answers"
def create_kitsune_questions():
  schema = [
    bigquery.SchemaField('question_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('question_content', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('created_date', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('creator_username', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('updated', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('updated_by', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('is_solved', 'BOOLEAN', mode='NULLABLE'),
    bigquery.SchemaField('locale', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('product', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('topic', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('solution', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('solved_by', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('num_votes', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('num_votes_past_week', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('last_answer', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('metadata_array', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('tags_array', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('answers', 'STRING', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('kitsune_questions_raw')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'kitsune_questions_raw'


def create_kitsune_word_frequencies():
  schema = [
    bigquery.SchemaField('kitsune_dt', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('kitsune_word', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('kitsune_freq', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('kitsune_word_frequencies')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'kitsune_word_frequencies'


def create_kitsune_sentiment():
  schema = [
      bigquery.SchemaField('quesiton_id', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('score', 'FLOAT', mode='NULLABLE',
                           description='Sentiment score from the google language api'),
      bigquery.SchemaField('magnitude', 'FLOAT', mode='NULLABLE',
                           description='Sentiment magnitude from the google language api'),
      bigquery.SchemaField('discrete_sentiment', 'STRING', mode='NULLABLE', 
                           description='''Contains a simple to understand aggregate score 
                                       of the sentiment such as positive or negative, based 
                                      on the score and magnitude'''),
      bigquery.SchemaField('topic', 'STRING', mode='NULLABLE',
                           description='The topic field from the Kitsune api')
  ]
  table_ref = dataset_ref.table('kitsune_sentiment')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)

def create_kitsune_driving_sentiment():
  schema = [
      bigquery.SchemaField('quesiton_id', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('topic', 'STRING', mode='NULLABLE',
                           description='The topic field from the Kitsune api')
  ]
  table_ref = dataset_ref.table('kitsune_driving_sentiment')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)

def main():

  #create_kitsune_answers()
  #create_kitsune_questions()
  create_kitsune_word_frequencies()
  create_kitsune_sentiment()
  create_kitsune_driving_sentiment()


if __name__ == '__main__':
  main()
