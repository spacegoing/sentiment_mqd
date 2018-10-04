# -*- coding: utf-8 -*-
from pymongo import MongoClient
import pandas as pd
import pytz

# todo: move to database
client = MongoClient('mongodb://localhost:27017/')
mkt_db = client['Guba_Posts']


def close_mongo_access():
  client.close()


def error_insert(item, meta):
  col_name = meta['stock_code']
  mkt_db[col_name + '_error_urls'].insert_one(item)


def post_insert(result, meta):
  col_name = meta['stock_code']
  mkt_db[col_name].insert_one(result)


def comment_update(result, meta):
  col_name = meta['stock_code']
  mkt_db[col_name].update_one({
      'post_url': meta['post_url']
  }, {"$push": {
      "comment_dict_list": {
          "$each": result
      }
  }})


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


db_handler_dict = {'post_insert': post_insert, 'comment_update': comment_update}
