from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
#import pandas as pd
import csv
from datetime import timedelta, date

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'sumo-imposing-union-227917-b3125c4ed9d8.json'
VIEW_ID = '65912487'


def daterange(start_date, end_date):
  for n in range(int ((end_date - start_date).days)):
    yield start_date + timedelta(n)

start_date = date(2018, 1, 1)
#start_date = date(2018,7,16)
end_date = date(2019, 3, 11) # choked after 20190305
#end_date = date(2018, 6, 1)

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

  
def run_total_users(analytics):
  #response = get_total_users(analytics, '2018-01-01', '2019-03-11')
  response = get_total_users(analytics, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
  with open("./ga_data_total_users.csv", "w", encoding='utf8') as f:
    csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    writer = csv.writer(f, dialect='myDialect')
    results = []
    fields = ["ga_date","ga_users"]
    results.append(fields)
    writer.writerows(add_response_to_results(response, results))


def run_search_ctr(analytics):
  response = get_total_users(analytics, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
  with open("./ga_data_search_ctr.csv", "w", encoding='utf8') as f:
    csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    writer = csv.writer(f, dialect='myDialect')
    results = []
    fields = ["ga_date","ga_segment","ga_users","ga_uniqueEvents"]
    results.append(fields)
    writer.writerows(add_response_to_results(response, results))


def run_inproduct_vs_organic(analytics):
  response = get_total_users(analytics, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
  with open("./ga_data_inproduct_vs_organic.csv", "w", encoding='utf8') as f:
    csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    writer = csv.writer(f, dialect='myDialect')
    results = []
    fields = ["ga_date","ga_segment","ga_users","ga_sessions"]
    results.append(fields)
    writer.writerows(add_response_to_results(response, results))

def run_kb_exit_rate(analytics):
  #end_date = date(2018, 4, 13) # choked after 20180412; change filenames!!
  
  results = []
  
  with open("./ga_data_top20_kb_exit_rate.csv", "w", encoding='utf8') as f:
    csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    writer = csv.writer(f, dialect='myDialect')
    
    fields = ["ga_date","ga_exitPagePath","ga_exitRate","ga_exits","ga_pageviews"]
    results.append(fields)
    writer.writerows(results)
    
    for dt in daterange(start_date, end_date):
      dt_str = dt.strftime("%Y-%m-%d")
      print(dt_str)
      results = []
      response = get_top20_kb_exit_rate(analytics, dt_str, dt_str)
      add_response_to_results(response, results)
      
      writer.writerows(results)


def run_users_by_country(analytics):
  # ended on 20180111 so start from 1/12; change filenames!!

  results = []

  with open("./ga_data_users_by_country.csv", "w", encoding='utf8') as f:
    csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    writer = csv.writer(f, dialect='myDialect')
    
    fields = ["ga_date","ga_country","ga_users"]
    results.append(fields)
    writer.writerows(results)
    
    for dt in daterange(start_date, end_date):
      dt_str = dt.strftime("%Y-%m-%d")
      print(dt_str)
      results = []
      response = get_users_by_country(analytics, dt_str, dt_str)
      add_response_to_results(response, results)
      
      writer.writerows(results)

    
def main():
  analytics = initialize_analyticsreporting()
  run_users_by_country(analytics)
  run_kb_exit_rate(analytics)
  #response = get_users_by_country(analytics, '2019-02-19', '2019-02-25')
  #print_response(response)
  run_inproduct_vs_organic(analytics)
  run_search_ctr(analytics)
  run_total_users(analytics)


if __name__ == '__main__':
  main()
