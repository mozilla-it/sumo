import SurveyGizmo #includes json lib
from datetime import datetime
import csv
import json
import base64
import os, argparse

def main():
  """
  Main entry point for calls to Survey Gizmo REST API.
  """
  parser = argparse.ArgumentParser(description="SUMO Survey Gizmo main arguments")
  parser.add_argument('--outdir', action='store', help='file output directory', type=str, default='.')
  args = parser.parse_args(args)
  
  start=datetime.now()
  
  api_token_fn = os.environ['SURVEYGIZMO_SECRETS'] #'survey_gizmo_api_keys_encoded'
  tokens =json.loads(base64.b64decode(open(api_token_fn, "rb").read()).decode('utf-8'))

  api_token = tokens['api_token']
  api_secret_key = tokens['api_secret_key']

  survey_id = '4669267'
  results_per_page = '500' # takes about 30min to download all pages
  api_url_base = 'https://restapi.surveygizmo.com/v5/survey/' + survey_id + '/surveyresponse.json'

  params = {'resultsperpage': results_per_page, 'api_token': api_token, 'api_secret_key': api_secret_key, 'page': str(1)}

  with open(args.outdir+"/csat_results.csv", "w") as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(SurveyGizmo.get_survey_data(api_url_base, params))
      
  print(datetime.now()-start)

if __name__ == '__main__':
  main()
