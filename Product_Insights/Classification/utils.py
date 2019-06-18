import re

def get_regex_pattern(keyword_list):
  pattern_str = r''
  for keyword in keyword_list:
    re_keyword = keyword.strip().replace(' ', r'\s')
    if re_keyword[0] == '_':
      re_keyword = r'({}\w+)'.format(re_keyword.replace('_', ''))
    elif re_keyword[-1] == '_':
      re_keyword = r'(\w+{})'.format(re_keyword.replace('_', ''))
    pattern_str += r'\b{}\b |'.format(re_keyword)
  pattern_str = pattern_str[0:-1]
  r = re.compile(pattern_str, flags=re.I | re.X)
  return(r)

def match_keywords(text, keywords):
  keyword_list = keywords.split(',')
  r = get_regex_pattern(keyword_list)
  results = r.findall(text)
  if len(set(results)) >= len(keyword_list):
    return(True)
  else:
    return(False)

def keywords_based_classifier(text, keywords_map):
  text_topics = []
  for i, row in keywords_map.iterrows():
    if match_keywords(text, row.keywords):
      text_topics.append(row.topics)      
  return(list(set(text_topics)))