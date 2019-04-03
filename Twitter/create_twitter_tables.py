# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('sumo')


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

  create_twitter_word_frequencies()
  create_twitter_antivirus_frequencies()


if __name__ == '__main__':
  main()
