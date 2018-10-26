# -*- coding: utf-8 -*-
import pickle
from pymongo import MongoClient
from collections import defaultdict
from functools import partial
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')
forum_db = client['Guba_Posts_2018']  # raw posts comments from guba
meta_db = client['Guba_Meta']  # stock codes
agg_db = client['Guba_Agg']  # agg results
stock_date_time_count_col = agg_db['stock_date_time_count']

# all_cols = forum_db.list_collection_names()
# all_cols = [forum_db[i] for i in all_cols if len(i) == 6]

SH_cols = [forum_db[i['stock_code']] for i in meta_db['SH'].find()]
SZ_cols = [forum_db[i['stock_code']] for i in meta_db['SZ'].find()]

cols = SH_cols
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
      date: df[date].dropna().to_dict()
  } for date in df]

  time_date_dict = df.to_dict()
  print(i / len(cols))
  break

time_date_dict = defaultdict(partial(defaultdict, int))
for i in series.iteritems():
  datetime = i[0]
  value = i[1]
  time_date_dict[datetime.time()][datetime.date()] = value
time_date_df = pd.DataFrame.from_dict(time_date_dict, orient='index')
