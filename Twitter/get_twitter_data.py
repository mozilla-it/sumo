import tweepy

from datetime import datetime

import pandas as pd
import csv

from google.cloud import bigquery
bq_client = bigquery.Client()
dataset_name = 'sumo'
dataset_ref = bq_client.dataset(dataset_name)

from google.cloud import storage
storage_client = storage.Client()
sumo_bucket = storage_client.get_bucket('moz-it-data-sumo')

consumer_key = os.environ['SUMO_TWITTER_CONSUMER_KEY'] 
consumer_secret = os.environ['SUMO_TWITTER_CONSUMER_SECRET'] 

def update_bq_table(uri, fn, table_name):

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
  blob = sumo_bucket.blob("twitter/" + fn)
  new_blob = sumo_bucket.rename_blob(blob, "twitter/processed/" + fn)
  print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))
  

def get_tweet_data_row(row):
  # return [ row['id_str'], row['created_at'], row['full_text'].replace("\n", "\\n"), row['user']["id"] ]
  return [ row.id_str, row.created_at, row.full_text.replace("\n", "\\n"), row.user.id, row.in_reply_to_status_id_str ]


# WARNING - only getting data that had FFx replies so there may be bias aka questions with Ffx mentions not accounted for - we can't get this history


def get_firefox_mentions(api):
  # uses standard search API standard which can only access last 7 days of data
  # get data using sinceId to ensure no duplicates

  # If results from a specific ID onwards are reqd, set since_id to that ID.
  # else default to no lower limit, go as far back as API allows
  qry_max_id = ("""SELECT max(id_str) max_id FROM {0} """).format(dataset_name + ".twitter_mentions")
  query_job = bq_client.query(qry_max_id)
  max_id_result = query_job.to_dataframe() 
  max_id = max_id_result['max_id'].values[0]
  print(max_id)

  #searchQuery = '#someHashtag'  # this is what we're searching for
  maxTweets = 10000000 # Some arbitrary large number

  tweetCount = 0
  print("Downloading max {0} tweets".format(maxTweets))

  # tweet_mode="extended" to include truncated tweets
  results = []
  
  try: 
    new_tweets = tweepy.Cursor(api.search,  q="@firefox", tweet_mode="extended", since_id=str(max_id)).items() 

    for tweet in new_tweets:
      tweet_row = get_tweet_data_row(tweet)
      #print(tweet.id_str)
      results.append(tweet_row) 
      
      tweetCount = tweetCount + 1
      if (tweetCount > maxTweets):
        break
    
    df = pd.DataFrame.from_records(results, columns=["id_str","created_at","full_text","user_id","in_reply_to_status_id_str"] )

    min_id_str = df['id_str'].min()
    max_id_str = df['id_str'].max()
    print('min: ' + min_id_str + ', max: ' + max_id_str)
    fn = 'twitter_data_mentions_' + str(min_id_str) + "_to_" + str(max_id_str) + '.csv'
    df.to_csv("/tmp/" + fn, index=False)
    print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fn))
    
    blob = sumo_bucket.blob("twitter/" + fn)
    blob.upload_from_filename("/tmp/" + fn)

    update_bq_table("gs://moz-it-data-sumo/twitter/", fn, 'twitter_mentions')  
    
  except tweepy.TweepError as e:
    # Just exit if any error
    print("some error : " + str(e))


def get_firefox_data(api):
  #get all tweets with id=firefox

  # If results from a specific ID onwards are reqd, set since_id to that ID.
  # else default to no lower limit, go as far back as API allows
  sinceId = None

  # If results only below a specific ID are, set max_id to that ID.
  # else default to no upper limit, start from the most recent tweet matching the search query.
  qry_max_id = ("""SELECT max(id_str) max_id FROM {0} """).format(dataset_name + ".twitter_reviews")
  query_job = bq_client.query(qry_max_id)
  max_id_result = query_job.to_dataframe() 
  max_id = max_id_result['max_id'].values[0]
  print(max_id)
  
  maxTweets = 10000000 # Some arbitrary large number

  tweetCount = 0
  print("Downloading max {0} tweets".format(maxTweets))

  # tweet_mode="extended" to include truncated tweets
  results = []
  
  try: 
    #new_tweets = tweepy.Cursor(api.user_timeline, screen_name='@firefox', tweet_mode="extended").items()
    #old_tweets = tweepy.Cursor(api.user_timeline, screen_name='@firefox', tweet_mode="extended", max_id=str(max_id - 1)).items() # max_id-1 to exclude max_id since that will have already been added in previous pass
    new_tweets = tweepy.Cursor(api.user_timeline, screen_name='@firefox', tweet_mode="extended", since_id=str(max_id)).items() # max_id-1 to exclude max_id since that will have already been added in previous pass
    for tweet in new_tweets:
 
      # if in_reply_to_status_id_str has number, then look up that info, else, put blanks for fields reply_text, reply created_at, reply_user_id. we wouldn't now what % goes un-replied anyway so...
      tweet_row = get_tweet_data_row(tweet)
      in_reply_to_status_id_str = tweet.in_reply_to_status_id_str
      #print(in_reply_to_status_id_str)
      if in_reply_to_status_id_str:
        try:
          reply_tweet = api.get_status(in_reply_to_status_id_str)
          tweet_row.extend([reply_tweet.text.replace("\n", "\\n"), reply_tweet.created_at, reply_tweet.user.id])
        except tweepy.TweepError as e:
          print("Error trying to get in_reply_to_status_id_str={0}: {1}", in_reply_to_status_id_str, str(e))
          tweet_row.extend(['', '', ''])
      else:
        tweet_row.extend(['', '', ''])
      
      results.append(tweet_row) #get_tweet_data_row(tweet))
      
      tweetCount = tweetCount + 1

      if (tweetCount > maxTweets):
        break
    
    df = pd.DataFrame.from_records(results, columns=["id_str","created_at","full_text","user_id","in_reply_to_status_id_str","in_reply_to_status_text","in_reply_to_status_created_at","in_reply_to_status_user_id"] )
    #  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    min_id_str = df['id_str'].min()
    max_id_str = df['id_str'].max()
    print('min: ' + min_id_str + ', max: ' + max_id_str)
    fn = 'twitter_data_' + str(min_id_str) + "_to_" + str(max_id_str) + '.csv'
    df.to_csv("/tmp/" + fn, index=False)
    print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fn))
    
    blob = sumo_bucket.blob("twitter/" + fn)
    blob.upload_from_filename("/tmp/" + fn)

    update_bq_table("gs://moz-it-data-sumo/twitter/", fn, 'twitter_reviews')  
    
  except tweepy.TweepError as e:
    # Just exit if any error
    print("some error : " + str(e))

  # created_at (UTC), id_str (int64 str), text (if truncated=true, get ), user.id
  # in_reply_to_status_id_strIf the represented Tweet is a reply, this field will contain the string representation of the original Tweet’s ID. Example:"in_reply_to_status_id_str":"114749583439036416"
  # in bq, have to get all tweets with non-null in_reply_to_status_id_str, get distinct set of those id_str that are "questions" (to ffx?) and calculate the min. timedelta to these questions amongst the responses
  # quoted?, retweeted?, favorited

  # twitter files should be bookmarked by start and end ids (inclusive)
  # use both the max_id and since_id parameters to minimize the amount of redundant data

  # twitter apis
  # if you can wait at least 30 seconds for a new Tweet to become available, then one of our search Tweet APIs may be a better fit. 
  # standard tier of APIs also provides a 7-day search endpoint that does not provide full-fidelity data and supports a smaller set of query operators.
  # When ("truncated": true), the "extended_tweet" fields should be parsed instead of the root-level fields.

  #user's @ handle. This will return tweets which mention @user and also tweets by @user


def main():
  # OAuth process, using the keys and tokens
  #auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  auth = tweepy.AppAuthHandler(consumer_key, consumer_secret) # appplication handler has higher limits
  #auth.set_access_token(access_token, access_token_secret)

  # Creation of the actual interface, using authentication
  api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)

  get_firefox_data(api)
  get_firefox_mentions(api)
  
  
if __name__ == '__main__':
	main()
