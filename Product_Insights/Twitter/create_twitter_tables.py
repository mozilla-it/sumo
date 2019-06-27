# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()

def create_twitter_sentiment(dataset_name, table_name):
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
  dataset_ref = client.dataset(dataset_name)
  table_ref = dataset_ref.table(table_name)
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)

def main(OUTPUT_DATASET, OUTPUT_TABLE):
  create_twitter_sentiment(OUTPUT_DATASET, OUTPUT_TABLE)

if __name__ == '__main__':
  main(OUTPUT_DATASET, OUTPUT_TABLE)
