# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('sumo')


def create_twitter_mentions():
  schema = [
    bigquery.SchemaField('id_str', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('created_at', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('full_text', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('user_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('in_reply_to_status_id_str', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('twitter_mentions')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'twitter_mentions'

  
def create_twitter_reviews():
  schema = [
    bigquery.SchemaField('id_str', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('created_at', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('full_text', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('user_id', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('in_reply_to_status_id_str', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('in_reply_to_status_text', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('in_reply_to_status_created_at', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('in_reply_to_status_user_id', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('twitter_reviews')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'twitter_reviews'
  

def create_twitter_word_frequencies():
  schema = [
    bigquery.SchemaField('tweet_dt', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('tweet_word', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('tweet_freq', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('twitter_word_frequencies')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'twitter_word_frequencies'


def create_twitter_antivirus_frequencies():
  schema = [
    bigquery.SchemaField('tweet_dt', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('tweet_word', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('tweet_freq', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('twitter_antivirus_frequencies')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'twitter_antivirus_frequencies'

    
def main():

  create_twitter_mentions()
  create_twitter_reviews()
  create_twitter_word_frequencies()
  create_twitter_antivirus_frequencies()


if __name__ == '__main__':
main()