# BQ table empty creation and schema defintions for GA tables
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('sumo')


#Package_Name:STRING,App_Version_Code:INTEGER,App_Version_Name:STRING,Reviewer_Language:STRING,Device:STRING,Review_Submit_Date_and_Time:TIMESTAMP,Review_Submit_Millis_Since_Epoch:INTEGER,Review_Last_Update_Date_and_Time:TIMESTAMP,Review_Last_Update_Millis_Since_Epoch:INTEGER,Star_Rating:INTEGER,Review_Title:STRING,Review_Text:STRING,Developer_Reply_Date_and_Time:TIMESTAMP,Developer_Reply_Millis_Since_Epoch:INTEGER,Developer_Reply_Text:STRING,Review_Link:STRING

def create_googleplaystore_reviews():
  schema = [
    bigquery.SchemaField('Package_Name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('App_Version_Code', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('App_Version_Name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Reviewer_Language', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Device', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Review_Submit_Date_and_Time', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('Review_Submit_Millis_Since_Epoch', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Review_Last_Update_Date_and_Time', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('Review_Last_Update_Millis_Since_Epoch', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Star_Rating', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Review_Title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Review_Text', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Developer_Reply_Date_and_Time', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('Developer_Reply_Millis_Since_Epoch', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Developer_Reply_Text', 'STRING', mode='NULLABLE'),    
    bigquery.SchemaField('Review_Link', 'STRING', mode='NULLABLE')
  ]
  table_ref = dataset_ref.table('googleplaystore_reviews_tmp')
  table = bigquery.Table(table_ref, schema=schema)
  table = client.create_table(table)  # API request

  assert table.table_id == 'googleplaystore_reviews_tmp'


# Create or overwrite the existing table if it exists
#table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
#table_schema = bq.Schema.from_data(simple_dataframe)
#table.create(schema = table_schema, overwrite = True)

    
def main():

  create_googleplaystore_reviews()


if __name__ == '__main__':
  main()
