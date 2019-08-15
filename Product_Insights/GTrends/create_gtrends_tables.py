# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()

def create_gtrends_queries(dataset_name, table_name):
  schema = [
      bigquery.SchemaField('update_date', 'DATE', mode='NULLABLE'),
      bigquery.SchemaField('region', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('original_query', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('translated_query', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('query_key_ts', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('search_increase_pct', 'INTEGER', mode='NULLABLE'),
  ]
  dataset_ref = client.dataset(dataset_name)
  table_ref = dataset_ref.table(table_name)
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)


def create_gtrends_timeseries(dataset_name, table_name):
  schema = [
      bigquery.SchemaField('query_key', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='NULLABLE'),
      bigquery.SchemaField('original_query', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('relative_search_volume', 'INTEGER', mode='NULLABLE'),
  ]
  dataset_ref = client.dataset(dataset_name)
  table_ref = dataset_ref.table(table_name)
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)

def create_tables(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES, OUTPUT_TABLE_TS):
  create_gtrends_queries(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES)
  create_gtrends_timeseries(OUTPUT_DATASET, OUTPUT_TABLE_TS)

if __name__ == '__main__':
  create_tables(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES, OUTPUT_TABLE_TS)
