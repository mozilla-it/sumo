import argparse

from Product_Insights.Twitter.process_twitter_data import process_data
from google.cloud import bigquery
from google.cloud import storage

def main():
  parser = argparse.ArgumentParser(description="Twitter sentiment and classification analysis main arguments")
  parser.add_argument('--projectid', nargs='?', const='.', type=str, help='GCP project id')

  parser.add_argument('--indataset', nargs='?', const='.', type=str, help='BigQuery input dataset')
  parser.add_argument('--intable', nargs='?', const='.', type=str, help='BigQuery input table')

  parser.add_argument('--outdataset', nargs='?', const='.', type=str, help='BigQuery output dataset')
  parser.add_argument('--outtable', nargs='?', const='.', type=str, help='BigQuery output table')

  parser.add_argument('--bucket', nargs='?', const='.', type=str, help='which gs bucket to save data to')
  parser.add_argument('--keywordsmap', nargs='?', const='.', type=str, 
                      help='location of csv containing keywords for classification. Only needed initially')

  args = parser.parse_args()

  PROJECT_ID = args.projectid

  INPUT_DATASET = args.indataset
  INPUT_TABLE = args.intable
  OUTPUT_DATASET = args.outdataset
  OUTPUT_TABLE = args.outtable

  OUTPUT_BUCKET = args.bucket

  KEYWORDS_FILE = args.keywordsmap


  # PROJECT_ID = 'marketing-1003'

  # INPUT_DATASET = 'sumo_views'
  # INPUT_TABLE = 'twitter_mentions_view'
  # OUTPUT_DATASET = 'analyse_and_tal'
  # OUTPUT_TABLE = 'twitter_sentiment'

  # OUTPUT_BUCKET = 'test-unique-bucket-name'

  #KEYWORDS_FILE = './Product_Insights/Classification/keywords_map.csv'

  bq_client = bigquery.Client(project=PROJECT_ID)
  storage_client = storage.Client(project=PROJECT_ID)

  process_data(INPUT_DATASET, INPUT_TABLE, OUTPUT_DATASET, OUTPUT_TABLE, OUTPUT_BUCKET, KEYWORDS_FILE)

if __name__ == '__main__':
  main()