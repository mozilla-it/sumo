# BQ table empty creation and schema defintions for SurveyGizmo tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('sumo')

			  
def create_surveygizmo():
  schema = [
    bigquery.SchemaField('Response ID', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Time Started', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('Date Submitted', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('Status', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Contact ID', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Language', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Referer', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('SessionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('User Agent', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Longitude', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Latitude', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Country', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('City', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('State/Region', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Postal', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Did you accomplish the goal of your visit?', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('How would you rate your experience with support.mozilla.org (Please help us by only rating the website and not Firefox)', 'STRING', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('surveygizmo')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'surveygizmo'


def main():

  create_surveygizmo()


if __name__ == '__main__':
  main()
