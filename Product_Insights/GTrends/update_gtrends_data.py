import argparse

from Product_Insights.GTrends.collect_gtrends_data import collect_data

def main():
  parser = argparse.ArgumentParser(description="Google Trends data collection main arguments")


  parser.add_argument('--outdataset', nargs='?', const='.', type=str, help='BigQuery output dataset')
  parser.add_argument('--outtablequeries', nargs='?', const='.', type=str, help='BigQuery output query table')
  parser.add_argument('--outtablets', nargs='?', const='.', type=str, help='BigQuery output query timeseries table')

  parser.add_argument('--bucket', nargs='?', const='.', type=str, help='which gs bucket to save data to')

  args = parser.parse_args()

  OUTPUT_DATASET = args.outdataset
  OUTPUT_TABLE_QUERIES = args.outtablequeries
  OUTPUT_TABLE_TS = args.outtablets

  OUTPUT_BUCKET = args.bucket

  collect_data(OUTPUT_DATASET, OUTPUT_TABLE_QUERIES, OUTPUT_TABLE_TS, OUTPUT_BUCKET)

if __name__ == '__main__':
  main()