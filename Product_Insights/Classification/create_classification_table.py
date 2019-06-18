# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('analyse_and_tal')

def create_keywords_map():
  schema = [
      bigquery.SchemaField('topic', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('keywords', 'STRING', mode='NULLABLE'),
  ]
  table_ref = dataset_ref.table('keywords_map')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)

def main():
  create_keywords_map()

if __name__ == '__main__':
  main()
