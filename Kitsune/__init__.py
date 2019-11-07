import json
import requests
import logging
import math
import pytz

from datetime import datetime, timedelta
from pytz import timezone

logger = logging.getLogger(__name__)


#https://support.mozilla.org/api/2/question?_method=GET

# questions to 0+ answers 	count=369008
# while has next page, process each page:
# results array (19 per page)
# .id, .content, .created, .creator.username, .is_solved, .locale, .product, .title, .topic, .solution, .solved_by, .num_votes, .num_votes_past_week
# .last_answer, .answers (array of id(s)), .tag (array of name, slug)
# metadata (array of key-value pairs) 

# answers always 1-1 to problems? count=1123210
# results array (19 per page)
# .id, .question (id), .content, .created, .creator.username, .updated, .updated_by, .is_spam, .num_helpful_votes, ,num_unhelpful_votes

# do delta? of max created date? or last updated date?

def convert_pst_to_utc(dt_str):
  fmt = '%Y-%m-%dT%H:%M:%SZ'
  pacific=pytz.timezone('US/Pacific')
  loc_dt = pacific.localize(datetime.strptime(dt_str,fmt))
  dt_utc = loc_dt.astimezone(pytz.utc) #dt.datetime.strptime(loc_dt, fmt)
  #print('dt_str:' + dt_str + ', dt_utc:' + dt_utc.strftime(fmt))
  return dt_utc.strftime(fmt)
    

def get_answer(survey_data_row, question_num, default):
    try:
        return survey_data_row[question_num]['answer']
    except:
        return default
        
        
#.id, .content, .created, .creator.username, 
#.is_solved, .locale, .product, .title, .topic, .solution, .solved_by, 
#.num_votes, .num_votes_past_week, .last_answer
# .answers (array of id(s)), 
#.tag (array of name, slug)
# metadata (array of key-value pairs) 

def get_question_data_row(row):
	return [row['id'], row['content'].replace("\n", "\\n"), convert_pst_to_utc(row['created']), row['creator']['username'], convert_pst_to_utc(row['updated']), row['updated_by'],
			row.get('is_solved',False), row.get('locale',''), row.get('product',''), row.get('title',''), row.get('topic',''),
			row.get('solution',''), row.get('solved_by',''), row.get('num_votes',0), row.get('num_votes_past_week',0),
			row.get('last_answer',''), row.get('metadata',''), row.get('tags',''), row.get('answers','') #, frt(row['created'], row.get('answers',''))
			]

# this assumes answers up to time T is always loaded first
def frt(question_created_dt, answers_array):
  question_created_dt = datetime.strptime(question_created_dt, '%Y-%m-%dT%H:%M:%SZ')
  print(question_created_dt) #"2019-03-05T13:40:09Z"
  frt = ''
  if answers_array:
    
    # do i need to parse answers_array to list of answers?
    print(str(len(answers_array)))
    print(answers_array)
    #for answer_id in answers_array:
    
  return str(frt)
  

def get_question_data(api_url_base, params):
	api_url = '{0}?_method=GET'.format(api_url_base)
	
	results = []
	response = requests.get(api_url, params=params)

	# need to get total_pages value and loop through to get all data &page=#
	fields = ["question_id","question_content","created_date","creator_username","updated","updated_by",
			  "is_solved","locale", "product", "title", "topic", "solution", "solved_by",
			  "num_votes","num_votes_past_week", "last_answer", "metadata_array", "tags_array", "answers"]

	results.append(fields)
			   
	if response.status_code == 200:
		raw = response.json()
		for i in raw['results']:
		  results.append(get_question_data_row(i))
		  #print(i['metadata'])

        #page=18464, next = null
		print(raw['count'])
		total_pages = math.ceil( raw['count'] / 20.0 )
		print(total_pages)
		
		#while raw['next'] is not None:
		for page in range(2,total_pages):
		  params['page'] = str(page)
		  print(page)
		  #print(raw['next'])
		  response = requests.get(api_url, params=params)
		  #response = requests.get(raw['next'])
		  
		  if response.status_code == 200:
		    #logger.info('status code 200')
		    raw = response.json()
		    for i in raw['results']:
		      results.append(get_question_data_row(i))
		  else:
		    print('[!] HTTP {0} calling [{1}]'.format(response.status_code, raw['next'])) # 401 unauthorized
		    #break

		logger.info('returning results')
		return results

	else:
		print('[!] HTTP {0} calling [{1}]'.format(response.status_code, api_url)) # 401 unauthorized
		return None



# .id, .question (id), .content, .created, .creator.username, .updated, .updated_by, 
#.is_spam, .num_helpful_votes, ,num_unhelpful_votes

def get_answer_data_row(row):
	return [row['id'], row.get('question',''), row['content'].replace("\n", "\\n"), convert_pst_to_utc(row['created']), row['creator']['username'],
			convert_pst_to_utc(row['updated']), row['updated_by'], row['is_spam'], row['num_helpful_votes'], row['num_unhelpful_votes']
			]

def get_answer_data(api_url_base, params):
	api_url = '{0}?_method=GET'.format(api_url_base)
	
	results = []
	response = requests.get(api_url, params=params)

	# need to get total_pages value and loop through to get all data &page=#
	fields = ["answer_id","question_id","answer_content","created_date","creator_username",
			  "updated","updated_by", "is_spam", "num_helpful_votes", "num_unhelpful_votes"]

	results.append(fields)
			   
	if response.status_code == 200:
		raw = response.json()
		for i in raw['results']:
		  results.append(get_answer_data_row(i))
			
		print(raw['count'])
		total_pages = int(math.ceil( raw['count'] / 20.0 ))
		print(total_pages)
		
		#while raw['next'] is not None:
		for page in range(2,total_pages):
		  params['page'] = str(page)
		  print(page)
		  #print(raw['next'])
		  response = requests.get(api_url, params=params)
		  #response = requests.get(raw['next'])
		  
		  if response.status_code == 200:
		    #logger.info('status code 200')
		    raw = response.json()
		    for i in raw['results']:
		      results.append(get_answer_data_row(i))
		  else:
		    print('[!] HTTP {0} calling [{1}]'.format(response.status_code, raw['next'])) # 401 unauthorized
		    #break

		logger.info('returning results')
		return results

	else:
		print('[!] HTTP {0} calling [{1}]'.format(response.status_code, api_url)) # 401 unauthorized
		return None

