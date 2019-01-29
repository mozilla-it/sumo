from unittest import TestCase

import SurveyGizmo
from SurveyGizmo.run_get_survey_data import main


# add sys.stdout

class TestSurveyGizmo(TestCase):
    def test_cmd(self):
        run_get_survey_data.main()

    def test_http_401_error(self):
      survey_id = '4669267'
      results_per_page = '500' # takes about 30min to download all pages
      api_url_base = 'https://restapi.surveygizmo.com/v5/survey/' + survey_id + '/surveyresponse.json'

      params = {'resultsperpage': results_per_page, 'page': str(1)}
      retValue = fSurveyGizmo.get_survey_data(params))
      self.assertIsNone(retValue)
