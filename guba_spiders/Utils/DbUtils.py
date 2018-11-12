# -*- coding: utf-8 -*-
import bson
from pymongo import MongoClient
import pandas as pd
import pytz

db_handler_dict = dict()

# todo: move to database
client = MongoClient('mongodb://localhost:27017/')
mkt_db = client['Guba_Posts_News']
client.drop_database('Guba_Meta')
meta_db = client['Guba_Meta']

# mongodb max document size
max_bson = 15 * 1024 * 1024


def regi_func(f):
  db_handler_dict[f.__name__] = f


# db handlers start
def error_insert(item, meta):
  col_name = meta['stock_code']
  mkt_db[col_name + '_error_urls'].insert_one(item)


def post_insert(result, meta):
  col_name = meta['stock_code']
  mkt_db[col_name].insert_one(result)


def comment_append(result, meta):
  '''
  At this stage only for first time scraping usage.

  For later stage maitainance (update) usage, following
  fields need to be changed:
  last_comment_time,
  is_root,
  doc_no,
  next_id

  potentially need to be changed:
  read_no,
  reply_no,
  '''
  col_name = meta['stock_code']  # '600000' big comment list
  url = meta['post_url']
  # url = "http://guba.eastmoney.com/news,600000,739093106,d.html" # big comment list
  # Although there are multi documents have same post_url
  # only the most recenty inserted document's
  # "next_id" field is ''
  most_recent_doc = mkt_db[col_name].find_one({"post_url": url, "next_id": ''})
  most_recent_doc['comment_dict_list'].append(result)
  doc_size = len(bson.BSON.encode(most_recent_doc))
  if doc_size > max_bson:  # create new document
    most_recent_doc_id = most_recent_doc.pop('_id')
    # print(most_recent_doc_id) # 5bd02a0847cc361a141b0068
    # prev_id = '5bd02a0847cc361a141b0068'
    # prev_recent_doc = mkt_db[col_name].find_one({"_id": bson.objectid.ObjectId(prev_id)})

    # new post doc; copy origin post content
    # update new post doc related meta data
    most_recent_doc['is_root'] = False
    most_recent_doc['doc_no'] += 1
    most_recent_doc['comment_dict_list'] = result
    insert_result = mkt_db[col_name].insert_one(most_recent_doc)
    new_id = insert_result.inserted_id
    # print(new_id) # 5bd02ad347cc361a141b0069

    # link new doc's id to previous most recent doc
    mkt_db[col_name].update_one({
        "_id": most_recent_doc_id
    }, {"$set": {
        "next_id": new_id
    }})
  else:  # append to most recent document
    mkt_db[col_name].update_one({
        'post_url': url,
        "next_id": ''
    }, {"$push": {
        "comment_dict_list": {
            "$each": result
        }
    }})


def stock_code_insert(result, meta):
  col_name = meta['market']
  meta_db[col_name].insert_many(result)


# db handlers end
# register function to db_handler_dict
_ = [
    regi_func(i)
    for i in [error_insert, post_insert, comment_append, stock_code_insert]
]


# Utils Other than db handlers
def get_latest_date_time(col_name, tzinfo, website_url=''):
  '''
  Parameters:
    col_name: single news source mkt should be uptick_name,
              multi sources mkt should be uptick_name_suffix
    tzinfo: string tzinfo
    website_url: Exchange.website_url. for multi-web sources
                 exchange to use
  Return:
    '' if database is empty
  '''
  col = mkt_db[col_name]
  cond = dict()
  if website_url:
    cond = {"website_url": {"$eq": website_url}}
  latest_list = list(col.find(cond).sort('date_time', -1).limit(1))
  latest_date = ''
  if latest_list:
    latest_date = latest_list[0]['date_time']
    tz = pytz.timezone(tzinfo)
    latest_date = pytz.utc.localize(latest_date, is_dst=None).astimezone(tz)
  return latest_date


def close_mongo_access():
  client.close()
