from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'sumo-imposing-union-227917-b3125c4ed9d8.json'
VIEW_ID = '65912487'


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
  
  
def get_product_vs_organic_users(analytics, startDate, endDate):
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
          'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
          'metrics': [{'expression': 'ga:users'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:sourceMedium'}]
        }]
      }
  ).execute() 
#metric ga:organicSearches ga:sessions ga:users
#dim {'name': 'ga:source'}, medium, ga:sourceMedium, ga:channelGrouping

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


def main():
  analytics = initialize_analyticsreporting()
  #response = get_users_by_country(analytics, '2019-02-19', '2019-02-25')
  #print_response(response)
  #response = get_total_users(analytics, '2019-02-19', '2019-02-25')
  #print_response(response)
  response = get_product_vs_organic_users(analytics, '2019-02-19', '2019-02-20')
  print_response(response)

if __name__ == '__main__':
  main()
