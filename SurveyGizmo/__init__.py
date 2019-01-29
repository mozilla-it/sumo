import json
import requests
import base64

def get_answer(survey_data_row, question_num, default):
    try:
        return survey_data_row[question_num]['answer']
    except:
        return default
        
def get_survey_data_row(row):
	return [row['id'], row['date_started'], row['date_submitted'], row['status'],
			row['contact_id'], row['language'],
			row['referer'], row['session_id'], row['user_agent'],
			row['longitude'],
			row['latitude'], row['country'], row['city'], row['region'], row['postal'],
			get_answer(row['survey_data'], str(2), ''),
			get_answer(row['survey_data'], str(4), '')]

def get_survey_data(api_url_base, params):
	api_url = '{0}?_method=GET'.format(api_url_base)
	
	results = []
	response = requests.get(api_url, params=params)

	# need to get total_pages value and loop through to get all data &page=#
	fields = ["Response ID","Time Started","Date Submitted","Status",
			  "Contact ID","Language", #"Legacy Comments","Comments",
			  "Referer","SessionID","User Agent", #"Extended Referer",
			  "Longitude", #
			  "Latitude","Country","City","State/Region","Postal",
			  "Did you accomplish the goal of your visit?", #2
			  "How would you rate your experience with support.mozilla.org (Please help us by only rating the website and not Firefox)", #4
			  ]

	results.append(fields)
			   
	if response.status_code == 200:
		raw = response.json()
		for i in raw['data']:
			results.append(get_survey_data_row(i))
			
		total_pages = raw['total_pages']
		print(total_pages)
		print(raw['total_count'])

		for page in range(2,3): #total_pages):
			params['page'] = str(page)
			print(page)
			response = requests.get(api_url, params=params)
			
			if response.status_code == 200:
				raw = response.json()
				for i in raw['data']:
					results.append(get_survey_data_row(i))

			else:
				print('[!] HTTP {0} calling [{1}]'.format(response.status_code, api_url)) # 401 unauthorized

		return results

	else:
		print('[!] HTTP {0} calling [{1}]'.format(response.status_code, api_url)) # 401 unauthorized
		return None

