import argparse

from Product_Insights.Kitsune.process_kitsune_data import process_data
from google.cloud import bigquery
from google.cloud import storage

def main():
  parser = argparse.ArgumentParser(description="Kitsune sentiment analysis main arguments")

  parser.add_argument('--indataset', nargs='?', const='.', type=str, help='BigQuery input dataset')
  parser.add_argument('--intable', nargs='?', const='.', type=str, help='BigQuery input table')

  parser.add_argument('--outdataset', nargs='?', const='.', type=str, help='BigQuery output dataset')
  parser.add_argument('--outtable', nargs='?', const='.', type=str, help='BigQuery output table')

  parser.add_argument('--bucket', nargs='?', const='.', type=str, help='which gs bucket to save data to')

  args = parser.parse_args()

  INPUT_DATASET = args.indataset
  INPUT_TABLE = args.intable
  OUTPUT_DATASET = args.outdataset
  OUTPUT_TABLE = args.outtable

  OUTPUT_BUCKET = args.bucket

  process_data(INPUT_DATASET, INPUT_TABLE, OUTPUT_DATASET, OUTPUT_TABLE, OUTPUT_BUCKET)

if __name__ == '__main__':
  main()