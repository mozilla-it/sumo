import mysql.connector
import os
import csv
import time
import ndjson
from mysql.connector import Error

import subprocess

import pytz
from datetime import datetime, timedelta
from pytz import timezone

from google.cloud import bigquery
bq_client = bigquery.Client()
dataset_name = 'sumo'
dataset_ref = bq_client.dataset(dataset_name)

from google.cloud import storage
storage_client = storage.Client()
sumo_bucket = storage_client.get_bucket('moz-it-data-sumo')


# because Kitsune datetime fields are in PST whereas BQ stores everything in UTC
def convert_pst_to_utc(dt):
  try:
    fmt = '%Y-%m-%dT%H:%M:%SZ'
    pacific=pytz.timezone('US/Pacific')
    #loc_dt = pacific.localize(datetime.strptime(dt_str,fmt))
    loc_dt = pacific.localize(dt)
    dt_utc = loc_dt.astimezone(pytz.utc) #dt.datetime.strptime(loc_dt, fmt)
    #print('dt_str:' + dt_str + ', dt_utc:' + dt_utc.strftime(fmt))
    return dt_utc.strftime(fmt)
  except (ValueError, TypeError, AttributeError):
    return ''


def create_tbl(schema, tbl_name):
  table_ref = dataset_ref.table(tbl_name) #'kitsune_kb_votes')
  table = bigquery.Table(table_ref, schema=schema)
  table = bq_client.create_table(table)  # API request

  assert table.table_id == tbl_name # 'kitsune_kb_votes'


def safe_cast(val, to_type, default=None):
    try:
        if val:
           return to_type(val)
        else:
           return default
    except (ValueError, TypeError):
        return default


def format_kb_votes(row):
    assert len(row)==5, "Invalid row length, expecting length 5: " + row
    return [ int(row[0]), str(row[1]), str(row[2]), convert_pst_to_utc(row[3]), int(row[4]) ]


def format_forums_forum(row):
    assert len(row)==7, "Invalid row length, expecting length 7: " + row
    return [ int(row[0]), str(row[1]), str(row[2]), str(row[3]), int(row[4]), int(row[5]), int(row[6]) ]


def format_forums_post(row):
    assert len(row)==7, "Invalid row length, expecting length 7: " + row
    #print(safe_cast(row[2],str,''))
    #print(safe_cast(row[2],str,'').replace("\n" , "\\n").replace("\r" , "\\n").replace("\r\n" , "\\n"))
    return [ safe_cast(row[0],int,None), safe_cast(row[1],int,None), safe_cast(row[2],str,'').replace("\n" , "\\n").replace("\r" , "\\n").replace("\r\n" , "\\n"), safe_cast(row[3],int,None), convert_pst_to_utc(row[4]), convert_pst_to_utc(row[5]), safe_cast(row[6],int,None) ]

def format_forums_thread(row):
    assert len(row)==9, "Invalid row length, expecting length 9: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],str,''), 
             safe_cast(row[2],int,None), 
             convert_pst_to_utc(row[3]), 
             safe_cast(row[4],int,None),
             safe_cast(row[5],int,None), 
             safe_cast(row[6],int,None),
             safe_cast(row[7],bool,None),
             safe_cast(row[8],bool,None)]


def format_customercare_reply(row):
    assert len(row)==8, "Invalid row length, expecting length 8: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],int,None), 
             safe_cast(row[2],str,''), 
             safe_cast(row[3],int,None), 
             safe_cast(row[4],str,''), 
             safe_cast(row[5],str,''), 
             convert_pst_to_utc(row[6]), 
             safe_cast(row[7],int,None) ]


def format_customercare_tweet(row):
    assert len(row)==6, "Invalid row length, expecting length 6: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],str,''), 
             safe_cast(row[2],str,''), 
             convert_pst_to_utc(row[3]), 
             safe_cast(row[4],int,None), 
             safe_cast(row[5],bool,None) ]


def format_users_profile(row):
    assert len(row)==8, "Invalid row length, expecting length 8: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],str,''), 
             safe_cast(row[2],str,''), 
             safe_cast(row[3],str,''), 
             safe_cast(row[4],str,''), 
             safe_cast(row[5],str,''), 
             safe_cast(row[6],str,''), 
             safe_cast(row[7],str,'')]


def format_wiki_document_products(row):
    assert len(row)==3, "Invalid row length, expecting length 3: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],int,None), 
             safe_cast(row[2],int,None)]


def format_wiki_document_topics(row):
    assert len(row)==3, "Invalid row length, expecting length 3: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],int,None), 
             safe_cast(row[2],int,None)]


def format_wiki_locale(row):
    assert len(row)==2, "Invalid row length, expecting length 2: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],str,"")]
             

def format_wiki_document(row):
    assert len(row)==17, "Invalid row length, expecting length 17: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],str,""),
             safe_cast(row[2],str,""),
             safe_cast(row[3],int,None), 
             safe_cast(row[4],int,None), 
             safe_cast(row[5],int,None), 
             safe_cast(row[6],str,"").replace("\n" , "\\n").replace("\r" , "\\n").replace("\r\n" , "\\n"),
             safe_cast(row[7],int,None),
             safe_cast(row[8],str,""),
             safe_cast(row[9],bool,None),
             safe_cast(row[10],bool,None),
             safe_cast(row[11],bool,None),
             safe_cast(row[12],bool,None),
             safe_cast(row[13],bool,None),
             safe_cast(row[14],str,""),
             safe_cast(row[15],str,""),
             safe_cast(row[16],int,None)
             ]
             

def format_wiki_document_contributors(row):
    assert len(row)==3, "Invalid row length, expecting length 3: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],int,None),
             safe_cast(row[2],int,None)]


def format_wiki_revision(row):
    assert len(row)==17, "Invalid row length, expecting length 17: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],int,None),
             safe_cast(row[2],str,"").replace("\n" , "\\n").replace("\r" , "\\n").replace("\r\n" , "\\n"),
             safe_cast(row[3],str,"").replace("\n" , "\\n").replace("\r" , "\\n").replace("\r\n" , "\\n"), 
             safe_cast(row[4],str,""), 
             convert_pst_to_utc(row[5]), 
             convert_pst_to_utc(row[6]),
             safe_cast(row[7],int,None),
             safe_cast(row[8],str,""),
             safe_cast(row[9],int,None),
             safe_cast(row[10],int,None),
             safe_cast(row[11],bool,None),
             safe_cast(row[12],int,None),
             safe_cast(row[13],bool,None),
             convert_pst_to_utc(row[14]),
             safe_cast(row[15],int,None),
             convert_pst_to_utc(row[16])
             ]


def format_auth_user(row):
    assert len(row)==10, "Invalid row length, expecting length 10: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],str,""),
             safe_cast(row[2],str,""),
             safe_cast(row[3],str,""), 
             safe_cast(row[4],str,""), 
             safe_cast(row[5],bool,None), 
             safe_cast(row[6],bool,None),
             safe_cast(row[7],bool,None),
             convert_pst_to_utc(row[8]),
             convert_pst_to_utc(row[9])
             ]


def format_products_product(row):
    assert len(row)==12, "Invalid row length, expecting length 12: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],str,""),
             safe_cast(row[2],str,""),
             safe_cast(row[3],int,None), 
             safe_cast(row[4],bool,None), 
             safe_cast(row[5],str,""), 
             safe_cast(row[6],str,""),
             safe_cast(row[7],int,None),
             safe_cast(row[8],str,""),
             safe_cast(row[9],int,None),
             safe_cast(row[10],str,""),
             safe_cast(row[11],str,"")
             ]


def format_wiki_helpfulvote(row):
    assert len(row)==7, "Invalid row length, expecting length 7: " + row
    return [ safe_cast(row[0],int,None), 
             safe_cast(row[1],int,None),
             safe_cast(row[2],bool,None),
             convert_pst_to_utc(row[3]), 
             safe_cast(row[4],int,None), 
             safe_cast(row[5],str,""), 
             safe_cast(row[6],str,"")
             ]


def update_bq_table(uri, fn, table_name, table_schema, write_disposition):

  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = write_disposition #"WRITE_TRUNCATE" #"WRITE_APPEND"  
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.maxBadRecords = 10
  job_config.skip_leading_rows = 1
  job_config.field_delimiter = "|"

  job_config.autodetect = False
  job_config.schema = table_schema
  
  orig_rows =  bq_client.get_table(table_ref).num_rows

  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request
  print("Starting job {}".format(load_job.job_id))

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  print('Loaded {} rows into {}:{}.'.format(destination_table.num_rows-orig_rows, 'sumo', table_name))
  
  # move fn to processed folder
  blob = sumo_bucket.blob("kitsune/" + fn)
  new_blob = sumo_bucket.rename_blob(blob, "kitsune/processed/" + fn)
  print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))


def update_bq_table_json(uri, fn, table_name, table_schema, write_disposition):

  table_ref = dataset_ref.table(table_name)
  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = write_disposition # "WRITE_TRUNCATE" #"WRITE_APPEND"  
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

  job_config.autodetect = False
  job_config.schema = table_schema

  orig_rows =  bq_client.get_table(table_ref).num_rows

  load_job = bq_client.load_table_from_uri(uri + fn, table_ref, job_config=job_config)  # API request
  print("Starting job {}".format(load_job.job_id))

  load_job.result()  # Waits for table load to complete.
  destination_table = bq_client.get_table(table_ref)
  print('Loaded {} rows into {}:{}.'.format(destination_table.num_rows-orig_rows, 'sumo', table_name))
  
  # move fn to processed folder
  blob = sumo_bucket.blob("kitsune/" + fn)
  new_blob = sumo_bucket.rename_blob(blob, "kitsune/processed/" + fn)
  print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))


def fetch_data(fn, query, fields, format_fn, upload):
    try:
        conn = mysql.connector.connect(host='127.0.0.1', port=3306,
                                       database='kitsune',
                                       user='root')
                                       #user='root',
                                       #password='Phantom22')
        if conn.is_connected():
                print('Connected to MySQL database')
    
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        data = []
        data.append(fields)
        rownum = 0
        for row in rows:
             #print(row)
             rownum = rownum + 1
             data.append(format_fn(row)) #format_kb_votes(row))
             if rownum % 100000 ==0:
                print(rownum)

        with open("/tmp/" + fn, "w", encoding='utf8') as f:
             csv.register_dialect('myDialect', delimiter = '|', skipinitialspace=True) #quoting=csv.QUOTE_ALL, 
             writer = csv.writer(f, dialect='myDialect')
             writer.writerows(data)

        if upload:
          CHUNK_SIZE = 128 * 1024 * 1024  # season to taste
          blob = sumo_bucket.blob("kitsune/" + fn, chunk_size=CHUNK_SIZE)
          blob.upload_from_filename("/tmp/" + fn)

        cursor.close()
    
    except Error as e:
        print(e)
    finally:
        pass
        #conn.close()


def fetch_data_json(fn, query, fields, format_fn, upload):
    try:
        conn = mysql.connector.connect(host='127.0.0.1', port=3306,
                                       database='kitsune',
                                       user='root')
                                       #password='Phantom22')
        if conn.is_connected():
                print('Connected to MySQL database')
    
        cursor = conn.cursor()
        cursor.execute(query)
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        print(row_headers)
        rows = cursor.fetchall()
        json_data=[]
        rownum = 0
        for row in rows:
          rownum = rownum + 1
          json_data.append(dict(zip(row_headers,row)))
          if rownum % 100000 ==0:
            print(rownum)
          
        with open("/tmp/" + fn, 'w') as f:
          ndjson.dump(json_data, f, default = convert_pst_to_utc) #, indent=4) # convert datetime to utc and format as str

        if upload:
          CHUNK_SIZE = 128 * 1024 * 1024  # season to taste
          blob = sumo_bucket.blob("kitsune/" + fn, chunk_size=CHUNK_SIZE)
          blob.upload_from_filename("/tmp/" + fn)

        cursor.close()
    
    except Error as e:
        print(e)
    finally:
        conn.close()


def get_kb_votes_data():
    kb_votes_schema = [ bigquery.SchemaField('document_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('locale', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('helpful', 'INTEGER', mode='NULLABLE')
                      ]
    kb_votes_query = ("SELECT b.document_id, c.title, c.locale, b.created, a.helpful "
                      "FROM wiki_helpfulvote a, wiki_revision b, wiki_document c "
                      "WHERE a.revision_id = b.id AND b.document_id = c.id AND b.created <= '2013-12-31'")
    kb_votes_fields = ["document_id","title","locale","created", "helpful"]

    #create_kitsune_kb_votes()
    fetch_data("kb_votes.csv", kb_votes_query, kb_votes_fields, format_kb_votes)
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "kb_votes.csv", 'kitsune_kb_votes', kb_votes_schema)


def get_forums_forum_data():
    forums_forum_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('name', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('slug', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('description', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('last_post_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('display_order', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('is_listed', 'INTEGER', mode='NULLABLE'),
                      ]
    forums_forum_query = ("SELECT * FROM forums_forum")
    forums_forum_fields = ["id","name","slug","description", "last_post_id", "display_order", "is_listed"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(forums_forum_schema, "kitsune_forums_forum")
    fetch_data("forums_forum.csv", forums_forum_query, forums_forum_fields, format_forums_forum, upload)
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "forums_forum.csv", 'kitsune_forums_forum', forums_forum_schema, write_disposition)


def get_forums_post_data():
    forums_post_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('thread_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('content', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('author_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('updated', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('updated_by_id', 'INTEGER', mode='NULLABLE'),
                      ]
    forums_post_query = ("SELECT * FROM forums_post")
    forums_post_fields = ["id","thread_id","content","author_id", "created", "updated", "updated_by_id"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(forums_post_schema, "kitsune_forums_post")
    fetch_data("forums_post.csv", forums_post_query, forums_post_fields, format_forums_post, upload)
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "forums_post.csv", 'kitsune_forums_post', forums_post_schema, write_disposition)


def get_forums_thread_data():
    forums_thread_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('forum_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('creator_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('last_post_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('replies', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('is_locked', 'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('is_sticky', 'BOOLEAN', mode='NULLABLE'),
                      ]
    forums_thread_query = ("SELECT * FROM forums_thread")
    forums_thread_fields = ["id","title","forum_id","created", "creator_id", "last_post_id", "replies", "is_locked", "is_sticky"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(forums_thread_schema, "kitsune_forums_thread")
    fetch_data("forums_thread.csv", forums_thread_query, forums_thread_fields, format_forums_thread, upload)
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "forums_thread.csv", 'kitsune_forums_thread', forums_thread_schema, write_disposition)


def get_kbforums_post_data():
    kbforums_post_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('thread_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('content', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('creator_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('updated', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('updated_by_id', 'INTEGER', mode='NULLABLE'),
                      ]
    kbforums_post_query = ("SELECT * FROM kbforums_post")
    kbforums_post_fields = ["id","thread_id","content","creator_id", "created", "updated", "updated_by_id"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(kbforums_post_schema, "kitsune_kbforums_post")
    fetch_data("kbforums_post.csv", kbforums_post_query, kbforums_post_fields, format_forums_post, upload) # use same format as forums_post
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "kbforums_post.csv", 'kitsune_kbforums_post', kbforums_post_schema, write_disposition)


def get_kbforums_thread_data():
    kbforums_thread_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('document_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('creator_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('last_post_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('replies', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('is_locked', 'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('is_sticky', 'BOOLEAN', mode='NULLABLE'),
                      ]
    kbforums_thread_query = ("SELECT * FROM kbforums_thread")
    kbforums_thread_fields = ["id","title","document_id","created", "creator_id", "last_post_id", "replies", "is_locked", "is_sticky"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(kbforums_thread_schema, "kitsune_kbforums_thread")
    fetch_data("kbforums_thread.csv", kbforums_thread_query, kbforums_thread_fields, format_forums_thread, upload) #use same format as forums_thread
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "kbforums_thread.csv", 'kitsune_kbforums_thread', kbforums_thread_schema, write_disposition)


def get_customercare_reply_data():
    customercare_reply_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('user_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('twitter_username', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('tweet_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('raw_json', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('locale', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('reply_to_tweet_id', 'INTEGER', mode='NULLABLE'),
                      ]
    customercare_reply_query = ("SELECT * FROM customercare_reply")
    customercare_reply_fields = ["id","user_id","twitter_username","tweet_id", "raw_json", "locale", "created", "reply_to_tweet_id"]

    #create_tbl(customercare_reply_schema, "kitsune_customercare_reply")
    #fetch_data("customercare_reply.csv", customercare_reply_query, customercare_reply_fields, format_customercare_reply) 
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "customercare_reply.csv", 'kitsune_customercare_reply', customercare_reply_schema)


def get_customercare_tweet_data():
    customercare_tweet_schema = [ bigquery.SchemaField('tweet_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('raw_json', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('locale', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
                        bigquery.SchemaField('reply_to_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('hidden', 'BOOLEAN', mode='NULLABLE')
                      ]
    customercare_tweet_query = ("SELECT * FROM customercare_tweet")
    customercare_tweet_fields = ["tweet_id","raw_json","locale","created", "reply_to_id", "hidden"]

    #create_tbl(customercare_tweet_schema, "kitsune_customercare_tweet")
    #fetch_data("customercare_tweet.csv", customercare_tweet_query, customercare_tweet_fields, format_customercare_tweet) 
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "customercare_tweet.csv", 'kitsune_customercare_tweet', customercare_tweet_schema)


def get_users_profile():
    users_profile_schema = [ bigquery.SchemaField('user_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('name', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('timezone', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('city', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('locale', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('mozillians', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('involved_from', 'DATE', mode='NULLABLE')
                      ]
    users_profile_query = ("SELECT user_id, name, timezone, country, city, locale, mozillians, STR_TO_DATE(involved_from,'%Y-%m-%d') as involved_from FROM users_profile")
    users_profile_fields = ["user_id","name","timezone","country", "city", "locale", "mozillians", "involved_from"]

    #create_tbl(users_profile_schema, "kitsune_users_profile")
    #fetch_data("sub_users_profile.csv", users_profile_query, users_profile_fields, format_users_profile) 
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "sub_users_profile.csv", 'kitsune_users_profile', users_profile_schema)


def get_wiki_document_products_data():
    wiki_document_products_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('document_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('product_id', 'INTEGER', mode='NULLABLE')
                      ]
    wiki_document_products_query = ("SELECT * FROM wiki_document_products")
    wiki_document_products_fields = ["id","document_id","product_id"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(wiki_document_products_schema, "kitsune_wiki_document_products")
    fetch_data("wiki_document_products.csv", wiki_document_products_query, wiki_document_products_fields, format_wiki_document_products, upload) 
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "wiki_document_products.csv", 'kitsune_wiki_document_products', wiki_document_products_schema, write_disposition)


def get_wiki_document_topics_data():
    wiki_document_topics_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('document_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('topic_id', 'INTEGER', mode='NULLABLE')
                      ]
    wiki_document_topics_query = ("SELECT * FROM wiki_document_topics")
    wiki_document_topics_fields = ["id","document_id","topic_id"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(wiki_document_topics_schema, "kitsune_wiki_document_topics")
    fetch_data("wiki_document_topics.csv", wiki_document_topics_query, wiki_document_topics_fields, format_wiki_document_topics, upload) 
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "wiki_document_topics.csv", 'kitsune_wiki_document_topics', wiki_document_topics_schema, write_disposition)


def get_wiki_locale_data():
    wiki_locale_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('locale', 'STRING', mode='NULLABLE')
                      ]
    wiki_locale_query = ("SELECT * FROM wiki_locale")
    wiki_locale_fields = ["id","locale"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(wiki_locale_schema, "kitsune_wiki_locale")
    fetch_data("wiki_locale.csv", wiki_locale_query, wiki_locale_fields, format_wiki_locale, upload) 
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "wiki_locale.csv", 'kitsune_wiki_locale', wiki_locale_schema, write_disposition)


def get_wiki_document_data():
    wiki_document_schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('locale', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('current_revision_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('latest_localizable_revision_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('parent_id', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('html', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('category', 'INTEGER', mode='NULLABLE'),
                        bigquery.SchemaField('slug', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('is_template', 'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('is_localizable', 'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('is_archived', 'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('allow_discussion', 'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('needs_change', 'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('needs_change_comment', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('share_link', 'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('display_order', 'INTEGER', mode='NULLABLE')
                      ]
    wiki_document_query = ("SELECT * FROM wiki_document")
    wiki_document_fields = ["id","title","locale","current_revision_id","latest_localizable_revision_id","parent_id","html","category","slug","is_template","is_localizable","is_archived","allow_discussion","needs_change","needs_change_comment","share_link","display_order"]
    upload = False
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(wiki_document_schema, "kitsune_wiki_document")
    fetch_data_json("wiki_document.json", wiki_document_query, wiki_document_fields, format_wiki_document, upload) 
    subprocess.run(["gsutil", "-m", "cp", "/tmp/wiki_document.json", "gs://moz-it-data-sumo/kitsune/"])
    update_bq_table_json("gs://moz-it-data-sumo/kitsune/", "wiki_document.json", 'kitsune_wiki_document', wiki_document_schema, write_disposition)


def get_wiki_document_contributors_data():
    schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('document_id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('user_id', 'INTEGER', mode='NULLABLE'),
                      ]
    query = ("SELECT * FROM wiki_document_contributors")
    fields = ["id","document_id","user_id"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(schema, "kitsune_wiki_document_contributors")
    fetch_data("wiki_document_contributors.csv", query, fields, format_wiki_document_contributors, upload) 
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "wiki_document_contributors.csv", 'kitsune_wiki_document_contributors', schema, write_disposition)


def get_wiki_revision_data():
    schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('document_id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('summary', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('content', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('keywords', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
               bigquery.SchemaField('reviewed', 'TIMESTAMP', mode='NULLABLE'),
               bigquery.SchemaField('significance', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('comment', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('reviewer_id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('creator_id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('is_approved', 'BOOLEAN', mode='NULLABLE'),
               bigquery.SchemaField('based_on_id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('is_ready_for_localization', 'BOOLEAN', mode='NULLABLE'),
               bigquery.SchemaField('readied_for_localization', 'TIMESTAMP', mode='NULLABLE'),
               bigquery.SchemaField('readied_for_localization_by_id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('expires', 'TIMESTAMP', mode='NULLABLE')
               ]
    query = ("SELECT * FROM wiki_revision")
    fields = ["id","document_id","summary","content","keywords","created","reviewed","significance","comment","reviewer_id","creator_id","is_approved","based_on_id","is_ready_for_localization","readied_for_localization","readied_for_localization_by_id","expires"]
    upload = False
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(schema, "kitsune_wiki_revision")
    fetch_data_json("wiki_revision.json", query, fields, format_wiki_revision, upload) 
    subprocess.run(["gsutil", "-m", "cp", "/tmp/wiki_revision.json", "gs://moz-it-data-sumo/kitsune/"])
    update_bq_table_json("gs://moz-it-data-sumo/kitsune/", "wiki_revision.json", 'kitsune_wiki_revision', schema, write_disposition)


def get_products_product_data():
    schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('description', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('display_order', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('visible', 'BOOLEAN', mode='NULLABLE'),
               bigquery.SchemaField('slug', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('image', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('image_offset', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('image_cachebuster', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('sprite_height', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('image_alternate', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('codename', 'STRING', mode='NULLABLE')
               ]
    query = ("SELECT * FROM products_product")
    fields = ["id","title","description","display_order","visible","slug","image","image_offset","image_cachebuster","sprite_height","image_alternate","codename"]
    upload = True
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(schema, "kitsune_products_product")
    fetch_data("products_product.csv", query, fields, format_products_product, upload) 
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "products_product.csv", 'kitsune_products_product', schema, write_disposition)


def get_wiki_helpfulvote_data():
    schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('revision_id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('helpful', 'BOOLEAN', mode='NULLABLE'),
               bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
               bigquery.SchemaField('creator_id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('anonymous_id', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('user_agent', 'STRING', mode='NULLABLE')
               ]
    
    # do this in 3 file chunks
    fields = ["id","revision_id","helpful","created","creator_id","anonymous_id","user_agent"]
    upload = False
    
    #create_tbl(schema, "kitsune_wiki_helpfulvote")
    
    write_disposition = "WRITE_TRUNCATE" 
    query = ("SELECT * FROM wiki_helpfulvote WHERE created <= '2013-12-31'")
    fetch_data("wiki_helpfulvote_1.csv", query, fields, format_wiki_helpfulvote, upload)
    subprocess.run(["gsutil", "-m", "cp", "/tmp/wiki_helpfulvote_1.csv", "gs://moz-it-data-sumo/kitsune/"])
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "wiki_helpfulvote_1.csv", 'kitsune_wiki_helpfulvote', schema, write_disposition)
    
    write_disposition = "WRITE_APPEND" 
    query = ("SELECT * FROM wiki_helpfulvote WHERE created > '2013-12-31' and created <= '2016-12-31'") 
    fetch_data("wiki_helpfulvote_2.csv", query, fields, format_wiki_helpfulvote, upload)
    subprocess.run(["gsutil", "-m", "cp", "/tmp/wiki_helpfulvote_2.csv", "gs://moz-it-data-sumo/kitsune/"])
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "wiki_helpfulvote_2.csv", 'kitsune_wiki_helpfulvote', schema, write_disposition)

    write_disposition = "WRITE_APPEND" 
    query = ("SELECT * FROM wiki_helpfulvote WHERE created > '2016-12-31'")
    fetch_data("wiki_helpfulvote_3.csv", query, fields, format_wiki_helpfulvote, upload)
    subprocess.run(["gsutil", "-m", "cp", "/tmp/wiki_helpfulvote_3.csv", "gs://moz-it-data-sumo/kitsune/"])
    update_bq_table("gs://moz-it-data-sumo/kitsune/", "wiki_helpfulvote_3.csv", 'kitsune_wiki_helpfulvote', schema, write_disposition)
    

def get_auth_user_data():
    schema = [ bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
               bigquery.SchemaField('username', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('first_name', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('last_name', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('email', 'STRING', mode='NULLABLE'),
               bigquery.SchemaField('is_staff', 'BOOLEAN', mode='NULLABLE'),
               bigquery.SchemaField('is_active', 'BOOLEAN', mode='NULLABLE'),
               bigquery.SchemaField('is_superuser', 'BOOLEAN', mode='NULLABLE'),
               bigquery.SchemaField('last_login', 'TIMESTAMP', mode='NULLABLE'),
               bigquery.SchemaField('date_joined', 'TIMESTAMP', mode='NULLABLE')
               ]
    query = ("SELECT id, username, first_name, last_name, email, is_staff, is_active, is_superuser, last_login, date_joined FROM auth_user")
    fields = ["id","username","first_name","last_name","email","is_staff","is_active","is_superuser","last_login","date_joined"]
    upload = False
    write_disposition = "WRITE_TRUNCATE" 
    
    #create_tbl(schema, "kitsune_auth_user")
    fetch_data_json("auth_user.json", query, fields, format_auth_user, upload) 
    subprocess.run(["gsutil", "-m", "cp", "/tmp/auth_user.json", "gs://moz-it-data-sumo/kitsune/"])
    update_bq_table_json("gs://moz-it-data-sumo/kitsune/", "auth_user.json", 'kitsune_auth_user', schema, write_disposition)


def main():

    # these can all be formatted, uploaded and updated db in one shot
#    get_forums_forum_data()
#    get_forums_post_data()
#    get_forums_thread_data()
#    get_kbforums_post_data()
#    get_kbforums_thread_data()
#    get_wiki_document_products_data()
#    get_wiki_document_topics_data()
#    get_wiki_document_contributors_data()
#    get_wiki_locale_data()
#    get_products_product_data()
#    
#    # these are large files that will take a while to run
#    get_wiki_document_data() # ndjsonified because of fields containing double quotes
#    get_wiki_revision_data()
#    get_auth_user_data()
#    
    # this take a mega long time to upload/run
    get_wiki_helpfulvote_data()
    # please check that mysql table rows match what's been loaded in bq esp for wiki_helpful vote
    
    # following are deprecated:
    #get_kb_votes_data() # no need for this join
    #get_customercare_reply_data( # not needed
    #get_customercare_tweet_data() # not needed
    #get_users_profile() # not needed

if __name__ == '__main__':
     main()
