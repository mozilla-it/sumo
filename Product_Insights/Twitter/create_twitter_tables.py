# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('analyse_and_tal')

def create_twitter_sentiment():
  schema = [
      bigquery.SchemaField('id_str', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('created_at', 'TIMESTAMP', mode='NULLABLE'),
      bigquery.SchemaField('full_text', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('user_id', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('in_reply_to_status_id_str', 'FLOAT', mode='NULLABLE'),
      bigquery.SchemaField('score', 'FLOAT', mode='NULLABLE',
                           description='Sentiment score from the google language api'),
      bigquery.SchemaField('magnitude', 'FLOAT', mode='NULLABLE',
                           description='Sentiment magnitude from the google language api'),
      bigquery.SchemaField('discrete_sentiment', 'STRING', mode='NULLABLE', 
                           description='''Contains a simple to understand aggregate score 
                                       of the sentiment such as positive or negative, based 
                                      on the score and magnitude'''),
      bigquery.SchemaField(
        'topics', 
        'RECORD', 
        mode='REPEATED',
        fields = [bigquery.SchemaField("topic", "STRING", mode="NULLABLE")]
        )
  ]
  table_ref = dataset_ref.table('twitter_sentiment')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)

def main():
  create_twitter_sentiment()

if __name__ == '__main__':
  main()
