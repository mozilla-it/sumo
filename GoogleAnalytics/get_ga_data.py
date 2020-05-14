
import argparse
import pandas as pd
import csv
from datetime import datetime, timedelta, date, timedelta
import time
import os
import re

#from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
#from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build

from google.cloud import bigquery
bq_client = bigquery.Client()
dataset_name = 'sumo'
dataset_ref = bq_client.dataset(dataset_name)

from google.cloud import storage
storage_client = storage.Client()

bucket = os.environ.get('BUCKET','moz-it-data-sumo')
sumo_bucket = storage_client.get_bucket(bucket)

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '/opt/secrets/secret.json'
VIEW_ID = '65912487'


def daterange(start_date, end_date):
  for n in range(int ((end_date - start_date).days)):
    yield start_date + timedelta(n)


def get_dimension_filter_clauses_desktop(dim_name):
    clauses = []
    dict_skeleton = {
        "operator": "EXACT",
        "dimensionName": dim_name,
    }
    with open("desktop_urls.txt") as f:
        for line in f:
            nodomain = re.sub("^https://support.mozilla.org", "", line.rstrip())
            dict_skeleton["expressions"] = [nodomain]
            clauses.append(dict_skeleton.copy())

    return(
        [{"filters": clauses}]
    )

def get_dimension_filter_clauses_fenix(dim_name):
    return([
      {"filters":
        [
          {
            "operator": "REGEXP",
            "dimensionName": dim_name,
            "expressions": [".*/kb/.*firefox-preview.*"]
          },
          {
            "operator": "PARTIAL",
            "dimensionName": dim_name,
            "expressions": ["/kb/firefox-sync-troubleshooting-and-tips"]
          },
          {
            "operator": "PARTIAL",
            "dimensionName": dim_name,
            "expressions": ["/kb/send-usage-data-firefox-mobile-browsers"]
          },
          {
            "operator": "PARTIAL",
            "dimensionName": dim_name,
            "expressions": ["/kb/firefox-sync-troubleshooting-and-tips"]
          },
        ]
      }
    ])

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """

  credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
  # Build the service object.
  #analytics = build('analyticsreporting', 'v4', credentials=credentials)

  #credentials = Credentials.from_service_account_file(KEY_FILE_LOCATION, scopes=SCOPES)
  #credentials = GoogleCredentials.get_application_default()
  #credentials = credentials.create_scoped(SCOPES)
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


def get_total_users_kb(analytics, startDate, endDate, subset_name):
  """Queries all users who have visited support.mozilla.org/kb/*
  """

  dimension_filter_clauses = [{"filters": [ {"operator": "PARTIAL", "dimensionName": "ga:pagePath", "expressions": ["/kb"]} ] }]
  if subset_name == "fenix":
    dimension_filter_clauses = get_dimension_filter_clauses_fenix("ga:pagePath")
  elif subset_name == "desktop":
    dimension_filter_clauses = get_dimension_filter_clauses_desktop("ga:pagePath")

  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}], #'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
          'metrics': [{'expression': 'ga:users'}],
          'dimensions': [{'name': 'ga:date'}],
          "dimensionFilterClauses": dimension_filter_clauses,
        }]
      }
  ).execute()

def get_inproduct_vs_organic(analytics, startDate, endDate, subset_name):
  """Queries the Analytics Reporting API V4.
  """

  segments = [{ "dynamicSegment": {
                  "name":"In-product",
                  "sessionSegment": {
                      "segmentFilters":[{
                          "simpleSegment": {
                              "orFiltersForSegment":[{
                                  "segmentFilterClauses":[{
                                      "dimensionFilter": {
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
              },{
                "dynamicSegment": {
                  "name":"Organic",
                  "sessionSegment": {
                      "segmentFilters":[{
                          "simpleSegment": {
                              "orFiltersForSegment":[{
                                  "segmentFilterClauses":[{
                                      "dimensionFilter": {
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

  dimension_filter_clauses = []
  if subset_name == "fenix":
    dimension_filter_clauses = get_dimension_filter_clauses_fenix("ga:pagePath")
  elif subset_name == "desktop":
    dimension_filter_clauses = get_dimension_filter_clauses_desktop("ga:pagePath")

  return analytics.reports().batchGet(
      body={
        'reportRequests': [{
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:segment'}],
          "dimensionFilterClauses": dimension_filter_clauses,
          "segments": segments,
        }]
    }).execute() 
#metric ga:organicSearches ga:sessions ga:users
#dim {'name': 'ga:source'}, medium, ga:sourceMedium, ga:channelGrouping
#inproduct

def get_inproduct_vs_organic_by_page(analytics, startDate, endDate, subset_name):
  """Queries the Analytics Reporting API V4.
  """

  segments = [{ "dynamicSegment": {
                  "name":"In-product",
                  "sessionSegment": {
                      "segmentFilters":[{
                          "simpleSegment": {
                              "orFiltersForSegment":[{
                                  "segmentFilterClauses":[{
                                      "dimensionFilter": {
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
              },{
                "dynamicSegment": {
                  "name":"Organic",
                  "sessionSegment": {
                      "segmentFilters":[{
                          "simpleSegment": {
                              "orFiltersForSegment":[{
                                  "segmentFilterClauses":[{
                                      "dimensionFilter": {
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

  dimension_filter_clauses = []
  if subset_name == "fenix":
    dimension_filter_clauses = get_dimension_filter_clauses_fenix("ga:pagePath")
  elif subset_name == "desktop":
    dimension_filter_clauses = get_dimension_filter_clauses_desktop("ga:pagePath")

  return analytics.reports().batchGet(
      body={
        'reportRequests': [{
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          #'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:sessions'}],
          'metrics': [{'expression': 'ga:pageviews'}, {'expression': 'ga:users'}, {'expression': 'ga:sessions'}],
          "orderBys": [{ "fieldName": 'ga:pageviews', "sortOrder": 'DESCENDING' }],
          #'dimensions': [{'name': 'ga:date'}, {'name': 'ga:segment'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:pagePath'}, {'name': 'ga:segment'}],
          "dimensionFilterClauses": dimension_filter_clauses,
          "segments": segments,
        }]
    }).execute() 

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


def get_kb_exit_rate(analytics, startDate, endDate, subset_name):
  """Queries the Analytics Reporting API V4.
  """
  dimension_filter_clauses = [{"filters": [ {"operator": "PARTIAL", "dimensionName": "ga:exitPagePath", "expressions": ["/kb"]} ] }]
  if subset_name == "fenix":
    dimension_filter_clauses = get_dimension_filter_clauses_fenix("ga:exitPagePath")
  elif subset_name == "desktop":
    dimension_filter_clauses = get_dimension_filter_clauses_desktop("ga:exitPagePath")

  return analytics.reports().batchGet(
      body={
        'reportRequests': [{
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:exitRate'}, {'expression': 'ga:exits'}, {'expression': 'ga:pageviews'}],
          #"metricFilterClauses": [{ object(MetricFilterClause) }],
          "orderBys": [{ "fieldName": 'ga:pageviews', "sortOrder": 'DESCENDING' }],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:exitPagePath'}],
          "dimensionFilterClauses": dimension_filter_clauses
          #"pageSize": 20
        }]
      }
  ).execute() 


def get_questions_exit_rate(analytics, startDate, endDate):
  """Queries the Analytics Reporting API V4.
     filter for [locale]/questions/[id]
     https://support.mozilla.org/en-US/questions/1256444
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
          "dimensionFilterClauses": [{"filters": [ {"dimensionName": "ga:exitPagePath","operator": "PARTIAL","expressions": ["/questions/"]} ] }],
          #"pageSize": 20
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

  
def run_total_users(analytics, start_dt, end_dt):
  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} """).format(dataset_name + ".ga_total_users")
  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() # no need to go through query_job.result()
  max_date = max_date_result['max_date'].values[0]

  # if start_date < max_date, then start_date=max_date  
  if max_date is not None and start_dt <= max_date: start_dt = max_date + timedelta(1)
  if end_dt<=max_date:
    print( ("run_total_users: End Date {0} <= Max Date {1}, no update needed.").format(end_dt,max_date) )
    return
  if start_dt>=end_dt:
    print( ("run_total_users: Start Date {0} >= End Date {1}, no update needed.").format(start_dt, end_dt) )
    return
  
  print( start_dt)
  
  fn = "ga_data_total_users_" + start_dt.strftime("%Y%m%d") + "_to_" + (end_dt - timedelta(days=1)).strftime("%Y%m%d") + ".csv"
  
  response = get_total_users(analytics, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
  results = []
  # write csv file to gs sumo folder for processing to BQ
  df = pd.DataFrame.from_records(add_response_to_results(response, results), columns=['ga_date', 'ga_users']) 
  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv("/tmp/" + fn, index=False)

  blob = sumo_bucket.blob("googleanalytics/" + fn)
  blob.upload_from_filename("/tmp/" + fn)
  
  update_bq_table("gs://{}/googleanalytics/".format(bucket), fn, 'ga_total_users')  


def run_total_users_kb(analytics, start_dt, end_dt, subset_name=""):

  suffix = ""
  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0}""") \
                .format(dataset_name + ".ga_total_users_kb")
  if len(subset_name) > 0:
    qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} WHERE product=\"{1}\"""") \
                   .format(dataset_name + ".ga_total_users_kb_by_product", subset_name)
    suffix = "_by_product"

  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() # no need to go through query_job.result()
  max_date = max_date_result['max_date'].values[0]

  # if start_date < max_date, then start_date=max_date  
  if max_date is not None and start_dt <= max_date: 
    start_dt = max_date + timedelta(1)
  if max_date and end_dt<=max_date:
    print( ("run_total_users_kb_{2}: End Date {0} <= Max Date {1}, no update needed.").format(end_dt,max_date,subset_name) )
    return
  if start_dt>=end_dt:
    print( ("run_total_users_kb_{2}: Start Date {0} >= End Date {1}, no update needed.").format(start_dt, end_dt, subset_name) )
    return
  
  print( start_dt)
  
  fn = "ga_data_total_users_kb_" + subset_name + "_" + start_dt.strftime("%Y%m%d") + \
       "_to_" + (end_dt - timedelta(days=1)).strftime("%Y%m%d") + ".csv"
  
  response = get_total_users_kb(analytics,
                                start_dt.strftime("%Y-%m-%d"),
                                end_dt.strftime("%Y-%m-%d"),
                                subset_name)
  results = []
  # write csv file to gs sumo folder for processing to BQ
  df = pd.DataFrame.from_records(add_response_to_results(response, results), columns=['ga_date', 'ga_users']) 
  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  if subset_name != "":
    df["product"] = subset_name
  df.to_csv("/tmp/" + fn, index=False)

  blob = sumo_bucket.blob("googleanalytics/" + fn)
  blob.upload_from_filename("/tmp/" + fn)
  
  update_bq_table("gs://{}/googleanalytics/".format(bucket), fn, 'ga_total_users_kb' + suffix)  


def run_search_ctr(analytics, start_dt, end_dt):
  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} """).format(dataset_name + ".ga_search_ctr")
  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() # no need to go through query_job.result()
  max_date = max_date_result['max_date'].values[0]

  # if start_date < max_date, then start_date=max_date  
  if start_dt <= max_date: start_dt = max_date + timedelta(1)
  if end_dt<=max_date:
    print( ("run_search_ctr: End Date {0} <= Max Date {1}, no update needed.").format(end_dt,max_date) )
    return
  if start_dt>=end_dt:
    print( ("run_search_ctr: Start Date {0} >= End Date {1}, no update needed.").format(start_dt, end_dt) )
    return
  
  print( start_dt)

  fn = "ga_data_search_ctr_" + start_dt.strftime("%Y%m%d") + "_to_" + (end_dt - timedelta(days=1)).strftime("%Y%m%d") + ".csv"

  response = get_search_ctr_users(analytics, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
  results = []
  # write csv file to gs sumo folder for processing to BQ
  df = pd.DataFrame.from_records(add_response_to_results(response, results), columns=["ga_date","ga_segment","ga_users","ga_uniqueEvents"]) 
  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv("/tmp/" + fn, index=False)
  
  blob = sumo_bucket.blob("googleanalytics/" + fn)
  blob.upload_from_filename("/tmp/" + fn)
  
  print('File {} uploaded to {}.'.format("/tmp/" + fn, "googleanalytics/" + fn))

  update_bq_table("gs://{}/googleanalytics/".format(bucket), fn, 'ga_search_ctr')  


def run_inproduct_vs_organic(analytics, start_dt, end_dt, subset_name=""):

  suffix = ""
  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0}""") \
                .format(dataset_name + ".ga_inproduct_vs_organic")
  if len(subset_name) > 0:
    qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} WHERE product=\"{1}\"""") \
                   .format(dataset_name + ".ga_inproduct_vs_organic_by_product", subset_name)
    suffix = "_by_product"

  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() # no need to go through query_job.result()
  max_date = max_date_result['max_date'].values[0]

  # if start_date < max_date, then start_date=max_date  
  if max_date and start_dt <= max_date: start_dt = max_date + timedelta(1)
  if max_date and end_dt<=max_date:
    print( ("run_inproduct_vs_organic_{2}: End Date {0} <= Max Date {1}, no update needed.").format(end_dt,max_date, subset_name) )
    return
  if start_dt>=end_dt:
    print( ("run_inproduct_vs_organic_{2}: Start Date {0} >= End Date {1}, no update needed.").format(start_dt, end_dt, subset_name) )
    return
    
  print( start_dt)
  
  fn = "ga_data_inproduct_vs_organic" + subset_name + "_" + start_dt.strftime("%Y%m%d") + "_to_" + (end_dt - timedelta(days=1)).strftime("%Y%m%d") + ".csv"

  response = get_inproduct_vs_organic(analytics, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"), subset_name)
  results = []
  # write csv file to gs sumo folder for processing to BQ
  df = pd.DataFrame.from_records(add_response_to_results(response, results), columns=["ga_date","ga_segment","ga_users","ga_sessions"]) 
  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  if subset_name != "":
    df["product"] = subset_name
  df.to_csv("/tmp/" + fn, index=False)

  blob = sumo_bucket.blob("googleanalytics/" + fn)
  blob.upload_from_filename("/tmp/" + fn)
  
  print('File {} uploaded to {}.'.format("/tmp/" + fn, "googleanalytics/" + fn))

  update_bq_table("gs://{}/googleanalytics/".format(bucket), fn, 'ga_inproduct_vs_organic' + suffix)  

def run_inproduct_vs_organic_by_page(analytics, start_dt, end_dt, subset_name=""):

  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} WHERE product=\"{1}\"""") \
                   .format(dataset_name + ".ga_inproduct_vs_organic_by_page", subset_name)

  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() # no need to go through query_job.result()
  max_date = max_date_result['max_date'].values[0]

  # if start_date < max_date, then start_date=max_date  
  if max_date and start_dt <= max_date: start_dt = max_date + timedelta(1)
  if max_date and end_dt<=max_date:
    print( ("run_inproduct_vs_organic_by_page_{2}: End Date {0} <= Max Date {1}, no update needed.").format(end_dt,max_date, subset_name) )
    return
  if start_dt>=end_dt:
    print( ("run_inproduct_vs_organic_by_page_{2}: Start Date {0} >= End Date {1}, no update needed.").format(start_dt, end_dt, subset_name) )
    return
    
  print( start_dt)
  
  fn = "ga_data_inproduct_vs_organic_by_page" + subset_name + "_" + start_dt.strftime("%Y%m%d") + "_to_" + (end_dt - timedelta(days=1)).strftime("%Y%m%d") + ".csv"

  response = get_inproduct_vs_organic_by_page(
                    analytics,
                    start_dt.strftime("%Y-%m-%d"), 
                    end_dt.strftime("%Y-%m-%d"), 
                    subset_name)
  results = []

  # write csv file to gs sumo folder for processing to BQ
  df = pd.DataFrame.from_records(
            add_response_to_results(response, results),
            #columns=["ga_date","ga_segment","ga_users","ga_sessions"]) 
            columns=["ga_date","ga_page_path","ga_segment","ga_pageviews","ga_users","ga_sessions"]) 
  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  if subset_name != "":
    df["product"] = subset_name
  df.to_csv("/tmp/" + fn, index=False)

  blob = sumo_bucket.blob("googleanalytics/" + fn)
  blob.upload_from_filename("/tmp/" + fn)
  
  print('File {} uploaded to {}.'.format("/tmp/" + fn, "googleanalytics/" + fn))

  update_bq_table("gs://{}/googleanalytics/".format(bucket), fn, 'ga_inproduct_vs_organic_by_page')  

def run_kb_exit_rate(analytics, start_dt, end_dt, subset_name=""):
  
  suffix = ""
  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0}""") \
                .format(dataset_name + ".ga_kb_exit_rate")
  if len(subset_name) > 0:
    qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} WHERE product=\"{1}\"""") \
                   .format(dataset_name + ".ga_kb_exit_rate_by_product", subset_name)
    suffix = "_by_product"

  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} """).format(dataset_name + ".ga_kb_exit_rate" + suffix)
  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() # no need to go through query_job.result()
  max_date = max_date_result['max_date'].values[0]

  # if start_date < max_date, then start_date=max_date  
  if max_date and start_dt <= max_date: start_dt = max_date + timedelta(1)
  if max_date and end_dt<=max_date:
    print( ("run_kb_exit_rate_{2}: End Date {0} <= Max Date {1}, no update needed.").format(end_dt,max_date,subset_name) )
    return
  if start_dt>=end_dt:
    print( ("run_kb_exit_rate_{2}: Start Date {0} >= End Date {1}, no update needed.").format(start_dt, end_dt,subset_name) )
    return
  
  fn = "ga_data_kb_exit_rate" + subset_name + "_" + start_dt.strftime("%Y%m%d") + "_to_" + (end_dt - timedelta(days=1)).strftime("%Y%m%d") + ".csv"

  df = pd.DataFrame()
  
  for dt in daterange(start_dt, end_dt):
    dt_str = dt.strftime("%Y-%m-%d")
    print(dt_str)
    results = []
    response = get_kb_exit_rate(analytics, dt_str, dt_str, subset_name)
    df = df.append( pd.DataFrame.from_records(add_response_to_results(response, results), columns=["ga_date","ga_exitPagePath","ga_exitRate","ga_exits","ga_pageviews"]) )
    time.sleep(2) # google api restrictions

  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  if subset_name != "":
    df["product"] = subset_name
  df.to_csv("/tmp/" + fn, index=False)
  
  blob = sumo_bucket.blob("googleanalytics/" + fn)
  blob.upload_from_filename("/tmp/" + fn)

  update_bq_table("gs://{}/googleanalytics/".format(bucket), fn, 'ga_kb_exit_rate' + suffix)  


def run_questions_exit_rate(analytics, start_dt, end_dt):
  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} """).format(dataset_name + ".ga_questions_exit_rate")
  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() # no need to go through query_job.result()
  max_date = max_date_result['max_date'].values[0]

  # if start_date < max_date, then start_date=max_date  
  if max_date and start_dt <= max_date: start_dt = max_date + timedelta(1)
  if max_date and end_dt<=max_date:
    print( ("run_questions_exit_rate: End Date {0} <= Max Date {1}, no update needed.").format(end_dt,max_date) )
    return
  if start_dt>=end_dt:
    print( ("run_questions_exit_rate: Start Date {0} >= End Date {1}, no update needed.").format(start_dt, end_dt) )
    return
    
  fn = "ga_data_questions_exit_rate_" + start_dt.strftime("%Y%m%d") + "_to_" + (end_dt - timedelta(days=1)).strftime("%Y%m%d") + ".csv"

  df = pd.DataFrame()
  
  for dt in daterange(start_dt, end_dt):
    dt_str = dt.strftime("%Y-%m-%d")
    print(dt_str)
    results = []
    response = get_questions_exit_rate(analytics, dt_str, dt_str)
    df = df.append( pd.DataFrame.from_records(add_response_to_results(response, results), columns=["ga_date","ga_exitPagePath","ga_exitRate","ga_exits","ga_pageviews"]) )
    time.sleep(2) # google api restrictions

  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv("/tmp/" + fn, index=False)

  blob = sumo_bucket.blob("googleanalytics/" + fn)
  blob.upload_from_filename("/tmp/" + fn)
  
  update_bq_table("gs://{}/googleanalytics/".format(bucket), fn, 'ga_questions_exit_rate')  

  
def run_users_by_country(analytics, start_dt, end_dt):
  qry_max_date = ("""SELECT max(ga_date) max_date FROM {0} """).format(dataset_name + ".ga_users_by_country")
  query_job = bq_client.query(qry_max_date)
  max_date_result = query_job.to_dataframe() # no need to go through query_job.result()
  max_date = max_date_result['max_date'].values[0]

  # if start_date < max_date, then start_date=max_date  
  if max_date and start_dt <= max_date: start_dt = max_date + timedelta(1)
  if max_date and end_dt<=max_date:
    print( ("run_users_by_country: End Date {0} <= Max Date {1}, no update needed.").format(end_dt,max_date) )
    return
  if start_dt>=end_dt:
    print( ("run_users_by_country: Start Date {0} >= End Date {1}, no update needed.").format(start_dt, end_dt) )
    return
  
  fn = "ga_data_users_by_country_" + start_dt.strftime("%Y%m%d") + "_to_" + (end_dt - timedelta(days=1)).strftime("%Y%m%d") + ".csv"

  df = pd.DataFrame()
  
  for dt in daterange(start_dt, end_dt):
    dt_str = dt.strftime("%Y-%m-%d")
    print(dt_str)
    results = []
    response = get_users_by_country(analytics, dt_str, dt_str)
    df = df.append( pd.DataFrame.from_records(add_response_to_results(response, results), columns=['ga_date', 'ga_country', 'ga_users']) )
    time.sleep(2) # google api restrictions

  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
  df.to_csv("/tmp/" + fn, index=False)
  
  blob = sumo_bucket.blob("googleanalytics/" + fn)
  blob.upload_from_filename("/tmp/" + fn)
  
  update_bq_table("gs://{}/googleanalytics/".format(bucket), fn, 'ga_users_by_country')  


def update_bq_table(uri, fn, table_name):

  # TODO: This operation is not idempotent. Running this job twice will load the
  # data twice! (I guess the max_date thing guards against that a bit?)
  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = "WRITE_APPEND"
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.skip_leading_rows = 1
  job_config.autodetect = True
  
  orig_rows =  bq_client.get_table(table_ref).num_rows

  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request
  print("Starting job {}".format(load_job.job_id))

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  print('Loaded {} rows into {}:{}.'.format(destination_table.num_rows-orig_rows, 'sumo', table_name))
  
  # move fn to processed folder
  blob = sumo_bucket.blob("googleanalytics/" + fn)
  new_blob = sumo_bucket.rename_blob(blob, "googleanalytics/processed/" + fn)
  print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))

    
def main(start_date=None, end_date=None):

    #start_date = date(2019, 4, 1) # inclusive
    #end_date = date(2019, 5, 1) # exclusive
    
    if start_date is None:
        start_date = date(2020, 4, 12) # inclusive
        end_date = datetime.today().date() # exclusive

    analytics = initialize_analyticsreporting()
  
    # construct filename from either start/end date vars or from BQ
  
    run_total_users(analytics, start_date, end_date)
  
    run_total_users_kb(analytics, start_date, end_date)

    run_total_users_kb(analytics, start_date, end_date, "fenix")

    run_total_users_kb(analytics, start_date, end_date, "desktop")
  
    run_users_by_country(analytics, start_date, end_date)
  
    run_inproduct_vs_organic(analytics, start_date, end_date)

    run_inproduct_vs_organic(analytics, start_date, end_date, "fenix")

    run_inproduct_vs_organic(analytics, start_date, end_date, "desktop")

    run_inproduct_vs_organic_by_page(analytics, start_date, end_date, "fenix")

    run_inproduct_vs_organic_by_page(analytics, start_date, end_date, "desktop")

    run_kb_exit_rate(analytics, start_date, end_date)

    run_kb_exit_rate(analytics, start_date, end_date, "fenix")

    run_kb_exit_rate(analytics, start_date, end_date, "desktop")

    run_questions_exit_rate(analytics, start_date, end_date)

    run_search_ctr(analytics, start_date, end_date)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SUMO Google Analytics main arguments")
    #parser.add_argument('-s', "--startdate", help="Inclusive Start Date - format YYYY-MM-DD ", required=False)
    #parser.add_argument('-e', "--enddate", help="Exclusive End Date format YYYY-MM-DD", required=False)
    #args = parser.parse_args()
    
    #hmmm no dedupe process
    # run historical kb_exit_rate and users_by_country in increments less than or equal to month so as not to hit API limits
    start_date = date(2020, 5, 10) # inclusive
    end_date = datetime.today().date() - timedelta(days=2) # exclusive
    
    main(start_date, end_date)
