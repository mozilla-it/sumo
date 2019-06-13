# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('analyse_and_tal')

def create_kitsune_sentiment():
  schema = [
      bigquery.SchemaField('question_id', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('score', 'FLOAT', mode='NULLABLE',
                           description='Sentiment score from the google language api'),
      bigquery.SchemaField('magnitude', 'FLOAT', mode='NULLABLE',
                           description='Sentiment magnitude from the google language api'),
      bigquery.SchemaField('discrete_sentiment', 'STRING', mode='NULLABLE', 
                           description='''Contains a simple to understand aggregate score 
                                       of the sentiment such as positive or negative, based 
                                      on the score and magnitude'''),
      bigquery.SchemaField('topic', 'STRING', mode='NULLABLE',
                           description='The topic field from the Kitsune api')
  ]
  table_ref = dataset_ref.table('kitsune_sentiment')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)


def main():
  create_kitsune_sentiment()


if __name__ == '__main__':
  main()
