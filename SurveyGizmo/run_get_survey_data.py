import SurveyGizmo #includes json lib
from datetime import datetime
import csv
import json
import base64
import os, argparse

def main(outdir):
  
  start=datetime.now()
  
  api_token = base64.b64decode(os.environ['SUMO_SURVEYGIZMO_TOKEN']).decode("utf-8").replace('\n', '')
  api_secret_key = base64.b64decode(os.environ['SUMO_SURVEYGIZMO_KEY']).decode("utf-8").replace('\n', '')

  survey_id = '4669267'
  results_per_page = '500' # takes about 30min to download all pages
  api_url_base = 'https://restapi.surveygizmo.com/v5/survey/' + survey_id + '/surveyresponse.json'

  params = {'resultsperpage': results_per_page, 'api_token': api_token, 'api_secret_key': api_secret_key, 'page': str(1)}

  with open(outdir + "/csat_results.csv", "w") as f:
      csv.register_dialect('myDialect', delimiter = ',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
      writer = csv.writer(f, dialect='myDialect')
      writer.writerows(SurveyGizmo.get_survey_data(api_url_base, params))
      
  print(datetime.now()-start)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="SUMO Survey Gizmo main arguments")
  parser.add_argument('--outdir', nargs='?', const='.', type=str, help='file output directory')
  args = parser.parse_args()
  
  print(args.outdir)
  main(args.outdir)
