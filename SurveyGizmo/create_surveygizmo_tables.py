# BQ table empty creation and schema defintions for SurveyGizmo tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('sumo')

def create_surveygizmo():
  schema = [
    bigquery.SchemaField('Response_ID', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Time_Started', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('Date_Submitted', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('Status', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Contact_ID', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Language', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Referer', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('SessionID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('User_Agent', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Longitude', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Latitude', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Country', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('City', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('State_Region', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Postal', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Did_you_accomplish_the_goal_of_your_visit_', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('How_would_you_rate_your_experience_with_support_mozilla_org__Please_help_us_by_only_rating_the_website_and_not_Firefox_', 'STRING', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('surveygizmo')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'surveygizmo'


def main():

  create_surveygizmo()


if __name__ == '__main__':
  main()
