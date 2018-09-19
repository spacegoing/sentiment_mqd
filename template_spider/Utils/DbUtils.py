# -*- coding: utf-8 -*-
from pymongo import MongoClient
import pandas as pd
import pytz

# todo: move to database
mkt_id_df = pd.read_csv('market_id.csv', index_col=None)
client = MongoClient('mongodb://localhost:27017/')
mkt_db = client['Market_Events']


def close_mongo_access():
  client.close()


def get_mkt_id(uptick_name):
  mkt_id_str = ''
  mkt_id_ser = mkt_id_df[mkt_id_df['uptick_name'] == uptick_name]['id']
  if not mkt_id_ser.empty:
    mkt_id_str = str(mkt_id_ser.iloc[0])
  return mkt_id_str


def get_filename(date_time, col_name, website_url=''):
  '''
  Parameters:
    date_time: timezone aware datetime
    col_name: single news source mkt should be uptick_name,
              multi sources mkt should be uptick_name_suffix
    website_url: Exchange.website_url. for multi-web sources
                 exchange to use
  '''
  col = mkt_db[col_name]
  date_time_str = date_time.strftime('%Y%m%d')
  mkt_id = get_mkt_id(col_name)

  def query_event_no(date_time):
    # convert to utc time (mongodb use utc time by default)
    cond = {"date_time": {"$gte": date_time, "$lte": date_time}}
    if website_url:
      cond = {"$and": [cond, {"website_url": {"$eq": website_url}}]}

    return col.find(cond).count()

  no = str(query_event_no(date_time) + 1)
  return mkt_id + 'd' + date_time_str + 'n' + no + '.pdf'


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
