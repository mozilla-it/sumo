# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('sumo')


#"answer_id","question_id","answer_content","created_date","creator_username",
#"updated","updated_by", "is_spam", "num_helpful_votes", "num_unhelpful_votes"
def create_release_calendar():
  schema = [
    bigquery.SchemaField('release', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('product', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('category', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('build_number', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('release_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('version', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('description', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('is_security_driven', 'BOOLEAN', mode='NULLABLE'),
    bigquery.SchemaField('date_utc', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('day_num', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('week_num', 'INTEGER', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('release_calendar')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'release_calendar'


def main():

  create_release_calendar()


if __name__ == '__main__':
  main()
