# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('sumo')


def create_ga_total_users():
  schema = [
    bigquery.SchemaField('ga_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('ga_users', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('ga_total_users')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'ga_total_users'


def create_ga_users_by_country():
  schema = [
    bigquery.SchemaField('ga_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('ga_country', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('ga_users', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('ga_users_by_country')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'ga_users_by_country'


def create_ga_inproduct_vs_organic():
  schema = [
    bigquery.SchemaField('ga_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('ga_segment', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('ga_users', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('ga_sessions', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('ga_inproduct_vs_organic')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'ga_inproduct_vs_organic'


def create_ga_kb_exit_rate():
  schema = [
    bigquery.SchemaField('ga_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('ga_exitPagePath', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('ga_exitRate', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('ga_exits', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('ga_pageviews', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('ga_kb_exit_rate')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'ga_kb_exit_rate'
  
  
def create_ga_questions_exit_rate():
  schema = [
    bigquery.SchemaField('ga_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('ga_exitPagePath', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('ga_exitRate', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('ga_exits', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('ga_pageviews', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('ga_questions_exit_rate')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'ga_questions_exit_rate'


def create_ga_search_ctr():
  schema = [
    bigquery.SchemaField('ga_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('ga_segment', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('ga_users', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('ga_uniqueEvents', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('ga_search_ctr')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'ga_search_ctr'

    
def main():
  
  create_ga_total_users()
  create_ga_users_by_country()
  create_ga_inproduct_vs_organic()
  create_ga_kb_exit_rate()
  create_ga_search_ctr()
  create_ga_questions_exit_rate()


if __name__ == '__main__':
  main()
