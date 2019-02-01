import SurveyGizmo #includes json lib
from datetime import datetime
import csv
import json
import base64
import os, re, argparse
import logging

logger = logging.getLogger(__name__)

def decode_base64(data, altchars=b'+/'):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.

    """
    data = re.sub(rb'[^a-zA-Z0-9%s]+' % altchars, b'', data)  # normalize
    #data = str(data)
    missing_padding = len(data) % 4
    if missing_padding:
        data += b'='* (4 - missing_padding)
    return base64.b64decode(data, altchars).decode("utf-8")

def main(outdir):
  
  start=datetime.now()
  
  logger.info('enc_tok:' + os.environ['SUMO_SURVEYGIZMO_TOKEN'] +'\n')
    
  with open("/tmp/out.csv", "w") as tmp_f:
    tmp_f.write( 'enc_tok:' + os.environ['SUMO_SURVEYGIZMO_TOKEN'] +'\n') 
    
  api_token = decode_base64(os.environ['SUMO_SURVEYGIZMO_TOKEN'].rstrip().encode("utf-8"))
  api_secret_key = decode_base64(os.environ['SUMO_SURVEYGIZMO_KEY'].rstrip().encode("utf-8"))
    
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
