import re

def get_regex_pattern(keyword_list):
  """ Constructs the regex pattern used to determine topics.

  There are four unique cases that are handled here. 
  1. If the keyword_list contains a single word or phrase, then
  regex pattern must detect this word or phrase exactly. 
  2. If the keyword_list contains multiple words/phrases, then
  the regex pattern must detect the presence of all words/phrases in keyword_list
  3. If the keyword_list contains words beginning with an underscore. 
  This returns a regex pattern that detects words beginning with the word following
  the underscore. Example: _load will match loading, but not download.
  4. If the keyword_list contains words ending with an underscore. 
  This returns a regex pattern that detects words ending with the word following
  the underscore. Example: load_ will match download, but not loading.
  """

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
  """ Detects if keywords are present in text. 

  When the keyword string contains multiple words then this function makes a distinction 
  based on the presence of a comma. 
  If the words are not separated by a comma, then the words are treated as a single phrase 
  which should be present in the text. 
  If the words are separated by a comma, then this is interpreted as multiple words that
  should be present in the text, but necessarily forming a phrase. 
  """

  keyword_list = keywords.split(',')
  r = get_regex_pattern(keyword_list)
  results = r.findall(text)
  if len(set(results)) >= len(keyword_list):
    return(True)
  else:
    return(False)

def keywords_based_classifier(text, keywords_map):
  """Determines topics based on the keywords in the keywords_map df.

  The function iterates over keyword and topic pairs found in keywords_map. 
  If the keywords are present in the text string, then the associated topic is 
  appended to the list of topics found. The function returns a list of unique topics found in the text string. 
  """ 

  text_topics = []
  for i, row in keywords_map.iterrows():
    if match_keywords(text, row.keywords):
      text_topics.append(row.topic)      
  return(list(set(text_topics)))