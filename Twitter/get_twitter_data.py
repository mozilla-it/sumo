import tweepy

from datetime import datetime

import pandas as pd
import csv

consumer_key = $CONSUMER_KEY
consumer_secret = $CONSUMER_SECRET


def get_tweet_data_row(row):
  # return [ row['id_str'], row['created_at'], row['full_text'].replace("\n", "\\n"), row['user']["id"] ]
  return [ row.id_str, row.created_at, row.full_text.replace("\n", "\\n"), row.user.id, row.in_reply_to_status_id_str ]


# WARNING - only getting data that had FFx replies so there may be bias aka questions with Ffx mentions not accounted for - we can't get this history


def get_firefox_mentions(api):
  # uses search API standard which can only access last 7 days of data
  # get data using sinceId to ensure no duplicates

  # If results from a specific ID onwards are reqd, set since_id to that ID.
  # else default to no lower limit, go as far back as API allows
  sinceId = '1109129628969508866' #None

  # If results only below a specific ID are, set max_id to that ID.
  # else default to no upper limit, start from the most recent tweet matching the search query.
  max_id = -1 #1105671493646774272 #-1 
  
  #searchQuery = '#someHashtag'  # this is what we're searching for
  maxTweets = 10000000 # Some arbitrary large number
  #tweetsPerQry = 100  # this is the max the API permits
  fName = 'twitter_data_mentions_since1109129628969508866_20190326.csv' # We'll store the tweets in a text file.

  tweetCount = 0
  print("Downloading max {0} tweets".format(maxTweets))

  # tweet_mode="extended" to include truncated tweets
  results = []
  
  try: 
    new_tweets = tweepy.Cursor(api.user_timeline, screen_name='@firefox', tweet_mode="extended", since_id=sinceId).items() # max_id-1 to exclude max_id since that will have already been added in previous pass
    #new_tweets = tweepy.Cursor(api.search,  q="@firefox", tweet_mode="extended").items()
    #new_tweets = tweepy.Cursor(api.search,  q="@firefox", tweet_mode="extended", max_id=str(max_id - 1)).items() # max_id-1 to exclude max_id since that will have already been added in previous pass
    for tweet in new_tweets:
      tweet_row = get_tweet_data_row(tweet)
      print(tweet.id_str)
      results.append(tweet_row) 
      
      tweetCount = tweetCount + 1
      if (tweetCount > maxTweets):
        break
    
    df = pd.DataFrame.from_records(results, columns=["id_str","created_at","full_text","user_id","in_reply_to_status_id_str"] )
    #  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    df.to_csv(fName, index=False)
    
  except tweepy.TweepError as e:
    # Just exit if any error
    print("some error : " + str(e))

  print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))



def get_firefox_data(api):
  #get all tweets with id=firefox

  # If results from a specific ID onwards are reqd, set since_id to that ID.
  # else default to no lower limit, go as far back as API allows
  sinceId = None

  # If results only below a specific ID are, set max_id to that ID.
  # else default to no upper limit, start from the most recent tweet matching the search query.
  max_id = -1 #1020677537897295872 #-1 #1009174211082870784 #1009105878811922432 
  
  #searchQuery = '#someHashtag'  # this is what we're searching for
  maxTweets = 10000000 # Some arbitrary large number
  #tweetsPerQry = 100  # this is the max the API permits
  fName = 'twitter_data.csv' # We'll store the tweets in a text file.

  tweetCount = 0
  print("Downloading max {0} tweets".format(maxTweets))

  # tweet_mode="extended" to include truncated tweets
  results = []
  
  try: 
    new_tweets = tweepy.Cursor(api.user_timeline, screen_name='@firefox', tweet_mode="extended").items()
    #new_tweets = tweepy.Cursor(api.user_timeline, screen_name='@firefox', tweet_mode="extended", max_id=str(max_id - 1)).items() # max_id-1 to exclude max_id since that will have already been added in previous pass
    for tweet in new_tweets:
 
      # if in_reply_to_status_id_str has number, then look up that info, else, put blanks for fields reply_text, reply created_at, reply_user_id. we wouldn't now what % goes un-replied anyway so...
      tweet_row = get_tweet_data_row(tweet)
      in_reply_to_status_id_str = tweet.in_reply_to_status_id_str
      print(in_reply_to_status_id_str)
      if in_reply_to_status_id_str:
        try:
          reply_tweet = api.get_status(in_reply_to_status_id_str)
          tweet_row.extend([reply_tweet.text.replace("\n", "\\n"), reply_tweet.created_at, reply_tweet.user.id])
        except tweepy.TweepError as e:
          print("Error trying to get in_reply_to_status_id_str={0}: {1}", in_reply_to_status_id_str, str(e))
          tweet_row.extend(['', '', ''])
      else:
        tweet_row.extend(['', '', ''])
      
      #print(len(tweet_row))
      #print(tweet_row)
      results.append(tweet_row) #get_tweet_data_row(tweet))
      
      tweetCount = tweetCount + 1
      #max_id = tweet.id
      #max_id = new_tweets[-1].id
      #print(str(max_id))
      
      if (tweetCount > maxTweets):
        break
    
    df = pd.DataFrame.from_records(results, columns=["id_str","created_at","full_text","user_id","in_reply_to_status_id_str","in_reply_to_status_text","in_reply_to_status_created_at","in_reply_to_status_user_id"] )
    #  df['ga_date'] = pd.to_datetime(df['ga_date'], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    df.to_csv(fName, index=False)
    
  except tweepy.TweepError as e:
    # Just exit if any error
    print("some error : " + str(e))

  print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))

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
  auth = tweepy.AppAuthHandler(consumer_key, consumer_secret) # appplication handler has higher limits

  # Creation of the actual interface, using authentication
  api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)

  get_firefox_mentions(api)

  
if __name__ == '__main__':
	main()
