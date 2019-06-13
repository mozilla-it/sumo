
dataset_ref = 'analyse_and_tal'
table_ref = 'twitter_sentiment'

def get_data(limit=10):
  query = ('SELECT * FROM `{0}.analyse_and_tal.twitter_sentiment` LIMIT {1}').format(PROJECT_ID, limit)
  try:
      df = bq_client.query(query).to_dataframe()
      return(df)
  except Exception as e:
      print(e)
      return(None)