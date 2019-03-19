from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import csv
from datetime import timedelta, date, timedelta
from google.cloud import bigquery

bq_client = bigquery.Client()
dataset_ref = bq_client.dataset('sumo')

from google.cloud import storage
storage_client = storage.Client()


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'sumo-imposing-union-227917-b3125c4ed9d8.json'
VIEW_ID = '65912487'


def daterange(start_date, end_date):
  for n in range(int ((end_date - start_date).days)):
    yield start_date + timedelta(n)

# run historical kb_exit_rate and users_by_country in monthy increments to not hit API limits
start_date = date(2018, 1, 1)
end_date = date(2019, 3, 12) 

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


def get_users_by_country(analytics, startDate, endDate):
  """Queries the Analytics Reporting API V4.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}], 
          'metrics': [{'expression': 'ga:users'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:country'}]
        }]
      }
  ).execute()


def get_total_users(analytics, startDate, endDate):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}], #'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
          'metrics': [{'expression': 'ga:users'}],
          #'metrics': [{'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:date'}]
        }]
      }
  ).execute()
  
  
def get_inproduct_vs_organic_old(analytics, startDate, endDate):
  """Queries the Analytics Reporting API V4.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [{
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:users'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:source'}],
          "dimensionFilterClauses": [{"filters": [ {"dimensionName": "ga:source","operator": "EXACT","expressions": ["organic"]} ] }]
        }]
      }
  ).execute() 
#metric ga:organicSearches ga:sessions ga:users
#dim {'name': 'ga:source'}, medium, ga:sourceMedium, ga:channelGrouping
#inproduct

def get_inproduct_vs_organic(analytics, startDate, endDate):
  """Queries the Analytics Reporting API V4.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [{
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:segment'}],
          "segments":[
      {
        "dynamicSegment":
        {
          "name":"In-product",
          "sessionSegment":
          {
            "segmentFilters":[
            {
              "simpleSegment":
              {
                "orFiltersForSegment":[
                {
                  "segmentFilterClauses":[
                  {
                    "dimensionFilter":
                    {
                      "dimensionName":"ga:source",
                      "expressions":["inproduct"],
                      "operator":"EXACT"
                    }
                  }]
                }]
              }
            }]
          }
        }
      },
      {
        "dynamicSegment":
        {
          "name":"Organic",
          "sessionSegment":
          {
            "segmentFilters":[
            {
              "simpleSegment":
              {
                "orFiltersForSegment":[
                {
                  "segmentFilterClauses":[
                  {
                    "dimensionFilter":
                    {
                      "dimensionName":"ga:medium",
                      "expressions":["organic"],
                      "operator":"EXACT"
                    }
                  }]
                }]
              }
            }]
          }
        }
      }]
    }]
  }).execute() 
#metric ga:organicSearches ga:sessions ga:users
#dim {'name': 'ga:source'}, medium, ga:sourceMedium, ga:channelGrouping
#inproduct

def get_search_ctr_users_old(analytics, startDate, endDate):
  """Queries the Analytics Reporting API V4.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [{
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:uniqueEvents'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:eventAction'}],
          "dimensionFilterClauses": [{"filters": [ {"dimensionName": "ga:eventAction","operator": "EXACT","expressions": ["Result Clicked"]} ] },
          							 {"filters": [ {"dimensionName": "ga:eventAction","operator": "EXACT","expressions": ["Search"]} ] }
          							 ]
        }]
      }
  ).execute() 
# % of people who searched and clicked (event) - metric is users not ga:uniqueEvents??


def get_search_ctr_users(analytics, startDate, endDate):
  """Queries the Analytics Reporting API V4.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [{
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:uniqueEvents'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:segment'}],
          "segments":[
      {
        "dynamicSegment":
        {
          "name":"Result Clicked",
          "sessionSegment":
          {
            "segmentFilters":[
            {
              "simpleSegment":
              {
                "orFiltersForSegment":[
                {
                  "segmentFilterClauses":[
                  {
                    "dimensionFilter":
                    {
                      "dimensionName":"ga:eventAction",
                      "expressions":["Result Clicked"],
                      "operator":"EXACT"
                    }
                  }]
                }]
              }
            }]
          }
        }
      },
      {
        "dynamicSegment":
        {
          "name":"Search",
          "sessionSegment":
          {
            "segmentFilters":[
            {
              "simpleSegment":
              {
                "orFiltersForSegment":[
                {
                  "segmentFilterClauses":[
                  {
                    "dimensionFilter":
                    {
                      "dimensionName":"ga:eventAction",
                      "expressions":["Search"],
                      "operator":"EXACT"
                    }
                  }]
                }]
              }
            }]
          }
        }
      }]
    }]
  }).execute() 
# % of people who searched and clicked (event) - metric is users not ga:uniqueEvents??

def get_top20_kb_exit_rate(analytics, startDate, endDate):
  """Queries the Analytics Reporting API V4.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [{
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:exitRate'}, {'expression': 'ga:exits'}, {'expression': 'ga:pageviews'}],
          #"metricFilterClauses": [{ object(MetricFilterClause) }],
          "orderBys": [{ "fieldName": 'ga:pageviews', "sortOrder": 'DESCENDING' }],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:exitPagePath'}],
          "dimensionFilterClauses": [{"filters": [ {"dimensionName": "ga:exitPagePath","operator": "PARTIAL","expressions": ["kb"]} ] }],
          "pageSize": 20
        }]
      }
  ).execute() 


def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])
      print(row)

      for header, dimension in zip(dimensionHeaders, dimensions):
        print(header + ': ' + dimension)

      for i, values in enumerate(dateRangeValues):
        #print('Date range: ' + str(i))
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print(metricHeader.get('name') + ': ' + value)


def add_response_to_results(response, results):

  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])
      #print(row)

      row = []
      for header, dimension in zip(dimensionHeaders, dimensions):
        #print(header + ': ' + dimension)
        row.append(dimension)

      for i, values in enumerate(dateRangeValues):
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          #print(metricHeader.get('name') + ': ' + value)
          row.append(value)
          
      #print(row)
      results.append(row)
  return results

  
def run_total_users(analytics, fn):
  response = get_total_users(analytics, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
  results = []
  # write csv file to gs sumo folder for processing to BQ
  df = pd.DataFrame.from_records(add_response_to_results(response, results), columns=['ga_date', 'ga_users']) 
  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv(fn, index=False)


def run_search_ctr(analytics, fn):
  response = get_search_ctr_users(analytics, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
  results = []
  # write csv file to gs sumo folder for processing to BQ
  df = pd.DataFrame.from_records(add_response_to_results(response, results), columns=["ga_date","ga_segment","ga_users","ga_uniqueEvents"]) 
  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv(fn, index=False)


def run_inproduct_vs_organic(analytics, fn):
  response = get_inproduct_vs_organic(analytics, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
  results = []
  # write csv file to gs sumo folder for processing to BQ
  df = pd.DataFrame.from_records(add_response_to_results(response, results), columns=["ga_date","ga_segment","ga_users","ga_sessions"]) 
  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv(fn, index=False)


def run_kb_exit_rate(analytics, fn):

  df = pd.DataFrame()
  
  for dt in daterange(start_date, end_date):
    dt_str = dt.strftime("%Y-%m-%d")
    print(dt_str)
    results = []
    response = get_top20_kb_exit_rate(analytics, dt_str, dt_str)
    df = df.append( pd.DataFrame.from_records(add_response_to_results(response, results), columns=["ga_date","ga_exitPagePath","ga_exitRate","ga_exits","ga_pageviews"]) )

  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv(fn, index=False)


def run_users_by_country(analytics, fn):

  df = pd.DataFrame()
  
  for dt in daterange(start_date, end_date):
    dt_str = dt.strftime("%Y-%m-%d")
    print(dt_str)
    results = []
    response = get_users_by_country(analytics, dt_str, dt_str)
    df = df.append( pd.DataFrame.from_records(add_response_to_results(response, results), columns=['ga_date', 'ga_country', 'ga_users']) )

  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv(fn, index=False)


def update_bq_table(fn, table_name):

  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.skip_leading_rows = 1
  job_config.autodetect = True

  with open(fn, 'rb') as source_file:
    job = bq_client.load_table_from_file(source_file,table_ref,location='US',job_config=job_config)  # API request

  job.result()  # Waits for table load to complete.
  # need to add some error check/handling if now all rows loaded
  print('Loaded {} rows into {}:{}.'.format(job.output_rows, 'sumo', table_name))

    
def main():
  analytics = initialize_analyticsreporting()
  
  # construct filename from either start/end date vars or from BQ
  
  #run_total_users(analytics,"./ga_data_total_users.csv")
  #update_bq_table("./ga_data_total_users.csv", 'ga_total_users')
  
  #fn = "./ga_data_users_by_country_" + start_date.strftime("%Y%m%d") + "_to_" + (end_date - timedelta(days=1)).strftime("%Y%m%d") + ".csv"
  #run_users_by_country(analytics, fn)
  #update_bq_table(fn, 'ga_users_by_country')
  
  #fn = "./ga_data_inproduct_vs_organic_" + start_date.strftime("%Y%m%d") + "_to_" + (end_date - timedelta(days=1)).strftime("%Y%m%d") + ".csv"
  #run_inproduct_vs_organic(analytics, fn)
  #update_bq_table(fn, 'ga_inproduct_vs_organic')

  #fn = "./ga_data_top20_kb_exit_rate_" + start_date.strftime("%Y%m%d") + "_to_" + (end_date - timedelta(days=1)).strftime("%Y%m%d") + ".csv"
  #run_kb_exit_rate(analytics, fn)
  #update_bq_table(fn, 'ga_kb_exit_rate')

  fn = "./ga_data_search_ctr_" + start_date.strftime("%Y%m%d") + "_to_" + (end_date - timedelta(days=1)).strftime("%Y%m%d") + ".csv"
  print("fn: " + fn)
  #run_search_ctr(analytics, fn)
  update_bq_table(fn, 'ga_search_ctr')
  

if __name__ == '__main__':
  main()
