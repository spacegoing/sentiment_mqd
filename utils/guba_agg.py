# -*- coding: utf-8 -*-
import pickle
from pymongo import MongoClient
from collections import defaultdict
from functools import partial
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')
mkt_db = client['Guba_Posts']
meta_db = client['Guba_Meta']
agg_db = client['Guba_Agg']

# all_cols = mkt_db.list_collection_names()
# all_cols = [mkt_db[i] for i in all_cols if len(i) == 6]

SH_cols = [mkt_db[i['stock_code']] for i in meta_db['SH'].find()]
SZ_cols = [mkt_db[i['stock_code']] for i in meta_db['SZ'].find()]


def get_date_stock_dict(all_cols):
  stock_date_num_dict = dict()
  for i, col in enumerate(all_cols):
    date_num_dict = defaultdict(int)
    for doc in col.find():
      date_num_dict[doc['post_datetime'].date()] += 1
      for c in doc['comment_dict_list']:
        date_num_dict[c['comment_time'].date()] += 1
    stock_date_num_dict[col.name] = date_num_dict
    print(col.name)
    print(i / len(all_cols))
  return stock_date_num_dict


def get_date_time_hist(all_cols):
  '''
  in 30 mins interval, how may seconds has comment
  '''
  date_dict = defaultdict(partial(defaultdict, int))
  for i, col in enumerate(all_cols):
    for doc in col.find():
      date_dict[doc['post_datetime'].date()][doc['post_datetime'].time()
                                             .strftime("%H:%M:%S")] += 1
      for c in doc['comment_dict_list']:
        date_dict[c['comment_time'].date()][c['comment_time'].time().strftime(
            "%H:%M:%S")] += 1

  date_df = pd.DataFrame(date_dict)
  date_df.index = pd.to_datetime(date_df.index)
  date_list = pd.date_range('09/01/2018', '09/30/2018')
  date_list = [i.date() for i in date_list]
  date_df = date_df.transpose()
  date_df.sort_index(inplace=True)
  date_df = date_df.loc[date_list[0]:date_list[-1]]
  date_df = date_df.transpose()
  date_df.sort_index(inplace=True)
  hist = date_df.groupby(pd.Grouper(freq='30Min')).count()
  hist['sum'] = hist.sum(axis=1)
  return hist


SH_hist = get_date_time_hist(SH_cols)
SZ_hist = get_date_time_hist(SZ_cols)
SH_hist.to_csv('SH_hist.csv')
SZ_hist.to_csv('SZ_hist.csv')


def get_ustocks_date_time_hist(all_cols):
  '''
  how many unique stocks has comments in each 30 mins
  '''
  date_time_stock_dict = defaultdict(partial(defaultdict, dict))
  for i, col in enumerate(all_cols):
    for doc in col.find():
      date_time_stock_dict[doc['post_datetime'].date()][doc[
          'post_datetime'].time().strftime("%H:%M:%S")][col.name] = 1
      for c in doc['comment_dict_list']:
        date_time_stock_dict[c['comment_time'].date()][c[
            'comment_time'].time().strftime("%H:%M:%S")][col.name] = 1

  date_dict = defaultdict(dict)
  for date, time_stock_dict in date_time_stock_dict.items():
    for time, stock_dict in time_stock_dict.items():
      date_dict[date][time] = len(stock_dict.keys())

  date_df = pd.DataFrame(date_dict)
  date_df.index = pd.to_datetime(date_df.index)
  date_list = pd.date_range('09/01/2018', '09/30/2018')
  date_list = [i.date() for i in date_list]
  date_df = date_df.transpose()
  date_df.sort_index(inplace=True)
  date_df = date_df.loc[date_list[0]:date_list[-1]]
  date_df = date_df.transpose()
  date_df.sort_index(inplace=True)
  hist = date_df.groupby(pd.Grouper(freq='30Min')).sum()
  hist['sum'] = hist.sum(axis=1)
  return hist


SH_ustocks_hist = get_ustocks_date_time_hist(SH_cols)
SZ_ustocks_hist = get_ustocks_date_time_hist(SZ_cols)
SH_ustocks_hist.to_csv('SH_ustocks_hist.csv')
SZ_ustocks_hist.to_csv('SZ_ustocks_hist.csv')

with open('SH_date_stock_dict.pickle', 'wb') as f:
  pickle.dump(get_date_stock_dict(SH_cols), f)

with open('SZ_date_stock_dict.pickle', 'wb') as f:
  pickle.dump(get_date_stock_dict(SZ_cols), f)

## Data Analysis
with open('SH_date_stock_dict.pickle', 'rb') as f:
  SH_stock_date_num_dict = pickle.load(f)

with open('SZ_date_stock_dict.pickle', 'rb') as f:
  SZ_stock_date_num_dict = pickle.load(f)


def sum_stats(stock_date_num_dict):
  date_list = pd.date_range('09/01/2018', '09/30/2018')
  date_list = [i.date() for i in date_list]

  stock_date_num_df = pd.DataFrame(stock_date_num_dict)
  stock_date_num_df = stock_date_num_df.loc[date_list[0]:date_list[-1]]

  monthly_each_stock_total = stock_date_num_df.sum(axis=0)
  monthly_each_stock_total.name = 'sum'
  stock_date_num_df = stock_date_num_df.append(monthly_each_stock_total)
  monthly_each_stock_total.nlargest(10)

  daily_all_stocks_total = stock_date_num_df.sum(axis=1)
  stock_date_num_df['sum'] = daily_all_stocks_total
  daily_all_stocks_total.nlargest(5)
  daily_all_stocks_total.nsmallest(5)

  # daily_top_10_stocks_dict = dict()
  # for d in date_list:
  #   d_list = []
  #   tmp_dict = stock_date_num_df.loc[d].nlargest(10).to_dict()
  #   for k, v in tmp_dict.items():
  #     d_list.append(k + ':' + str(int(v)))
  #   daily_top_10_stocks_dict[d] = d_list

  # daily_top_10_stocks_df = pd.DataFrame(daily_top_10_stocks_dict)

  return stock_date_num_df


SH_stock_date_num_df = sum_stats(SH_stock_date_num_dict)
SH_stock_date_num_df.to_csv('SH_date_stock_count.csv')

SZ_stock_date_num_df = sum_stats(SZ_stock_date_num_dict)
SZ_stock_date_num_df.to_csv('SZ_date_stock_count.csv')
'''
in each time interval (30 mins)
how many stocks been discussed
how many discussions
'''
