# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
OUTPUT_DATASET='analyse_and_tal'
OUTPUT_TABLE='keywords_map'

def create_keywords_map(dataset_name, table_name):
  schema = [
      bigquery.SchemaField('topic', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('keywords', 'STRING', mode='NULLABLE'),
  ]
  dataset_ref = client.dataset(dataset_name)
  table_ref = dataset_ref.table(table_name)
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)
  print('hey')

def main(OUTPUT_DATASET, OUTPUT_TABLE):
  create_keywords_map(OUTPUT_DATASET, OUTPUT_TABLE)

if __name__ == '__main__':
  main(OUTPUT_DATASET, OUTPUT_TABLE)
