# -*- coding: utf-8 -*-
import pickle
from pymongo import MongoClient
from collections import defaultdict
from functools import partial
import pandas as pd
import dateparser as dp

client = MongoClient('mongodb://localhost:27017/')
forum_db = client['Guba_Posts_2018']  # raw posts comments from guba
meta_db = client['Guba_Meta']  # stock codes
agg_db = client['Guba_Agg']  # agg results
stock_date_time_count_col = agg_db['stock_date_time_count']

# all_cols = forum_db.list_collection_names()
# all_cols = [forum_db[i] for i in all_cols if len(i) == 6]

SH_cols = [forum_db[i['stock_code']] for i in meta_db['SH'].find()]
SZ_cols = [forum_db[i['stock_code']] for i in meta_db['SZ'].find()]


def parse_date(date_str):
  return dp.parse(date_str, {'%Y-%m-%d'})


def insert_stock_date_time_count(cols):
  for i, col in enumerate(cols):
    datetime_counter = defaultdict(int)
    for doc in col.find():
      if doc['is_root']:
        datetime_counter[doc['post_datetime']] += 1
        # only count post time for once if it is root
        # because some long comment list post have
        # multi non root docs
      for c in doc['comment_dict_list']:
        datetime_counter[c['comment_time']] += 1

    # split datetime series into a
    # time index, date column dataframe
    s = pd.Series(datetime_counter)
    df = pd.pivot(s.index.strftime('%H:%M:%S'), s.index.strftime('%Y-%m-%d'), s)
    date_time_list = [{
        'stock_code': col.name,
        'date': parse_date(date),
        'time_count_list': df[date].dropna().to_dict()
    } for date in df]
    stock_date_time_count_col.insert_many(date_time_list)
    print('Finished: ', i / len(cols))


def get_stats(cols):
  be_date = parse_date('2018-01-01')
  en_date = parse_date('2018-06-30')
  date_postcount_dict = defaultdict(int)
  date_useccount_dict = defaultdict(int)
  time_postcount_ser = pd.Series()
  time_useccount_ser = pd.Series()

  def get_time_useccount_ser(ser, time_useccount_ser):
    ser.index = pd.to_datetime(ser.index)
    hist_sec = ser.resample('30Min').count()
    hist_sec.index = hist_sec.index.strftime('%H:%M:%S')
    unique_ser = pd.DataFrame(hist_sec).any(1) * 1
    time_useccount_ser = time_useccount_ser.add(unique_ser, fill_value=0)
    return time_useccount_ser

  for i, col in enumerate(cols):
    for j, doc in enumerate(
        stock_date_time_count_col.find({
            "date": {
                "$gte": be_date,
                "$lte": en_date
            },
            "stock_code": col.name
        })):
      ser = pd.Series(doc['time_count_list'])
      date_postcount_dict[doc['date']] += ser.sum()
      date_useccount_dict[doc['date']] += 1
      time_postcount_ser = time_postcount_ser.add(ser.iloc[:10], fill_value=0)
      time_useccount_ser = get_time_useccount_ser(ser, time_useccount_ser)
    print('Finished: ', i / len(cols))

  date_postcount_ser, date_useccount_ser = [
      pd.Series(i) for i in [date_postcount_dict, date_useccount_dict]
  ]
  date_postcount_ser.index.name = 'date'
  date_useccount_ser.index.name = 'date'
  time_postcount_ser.index.name = 'time'
  time_useccount_ser.index.name = 'time'
  return date_postcount_ser, date_useccount_ser, time_postcount_ser, time_useccount_ser


insert_stock_date_time_count(SH_cols)
date_postcount_ser, date_useccount_ser, time_postcount_ser, time_useccount_ser = get_stats(
    SH_cols)
date_postcount_ser.to_csv('SH_date_postcount.csv')
date_useccount_ser.to_csv('SH_date_usercount.csv')
time_postcount_ser.to_csv('SH_time_usercount.csv')
time_useccount_ser.to_csv('SH_time_usercount.csv')

insert_stock_date_time_count(SZ_cols)
date_postcount_ser, date_useccount_ser, time_postcount_ser, time_useccount_ser = get_stats(
    SZ_cols)
date_postcount_ser.to_csv('SZ_date_postcount.csv')
date_useccount_ser.to_csv('SZ_date_usercount.csv')
time_postcount_ser.to_csv('SZ_time_usercount.csv')
time_useccount_ser.to_csv('SZ_time_usercount.csv')
