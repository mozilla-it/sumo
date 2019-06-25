# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
OUTPUT_DATASET='analyse_and_tal'
OUTPUT_TABLE='kitsune_sentiment'

def create_kitsune_sentiment(dataset_name, table_name):
  schema = [
      bigquery.SchemaField('question_id', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('question_content', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('created_date', 'TIMESTAMP', mode='NULLABLE'),
      bigquery.SchemaField('creator_username', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('updated', 'TIMESTAMP', mode='NULLABLE'),
      bigquery.SchemaField('updated_by', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('is_solved', 'BOOLEAN', mode='NULLABLE'),
      bigquery.SchemaField('locale', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('product', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('topic', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('solved_by', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('num_votes', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('num_votes_past_week', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('metadata_array', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('tags_array', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('answers', 'STRING', mode='NULLABLE'),    
      bigquery.SchemaField('score', 'FLOAT', mode='NULLABLE',
                           description='Sentiment score from the google language api'),
      bigquery.SchemaField('magnitude', 'FLOAT', mode='NULLABLE',
                           description='Sentiment magnitude from the google language api'),
      bigquery.SchemaField('discrete_sentiment', 'STRING', mode='NULLABLE', 
                           description='''Contains a simple to understand aggregate score 
                                       of the sentiment such as positive or negative, based 
                                      on the score and magnitude'''),
  ]
  dataset_ref = client.dataset(dataset_name)
  table_ref = dataset_ref.table(table_name)
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)


def main():
  create_kitsune_sentiment(OUTPUT_DATASET, OUTPUT_TABLE)

if __name__ == '__main__':
  main()
