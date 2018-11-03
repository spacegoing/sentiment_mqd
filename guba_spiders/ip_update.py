# -*- coding: utf-8 -*-
import time
import datetime
import requests
import dateparser as dp
import pytz
from Utils import DbUtils as db


def get_ip_list(num):
  tmp_url = 'http://webapi.http.zhimacangku.com/getip?num=%d&type=2&pro=&city=0&yys=0&port=1&time=1&ts=1&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
  resp = requests.get(url=(tmp_url % (num)))
  json_data = resp.json()  # Check the JSON Response Content documentation below
  ip_list = json_data['data']
  return ip_list


def update_ip_list():
  try:
    bjtz = pytz.timezone('Asia/Shanghai')
    sydtz = pytz.timezone('Australia/Sydney')

    to_remove_list = []
    for i in db.proxy_col.find():
      exp_time = dp.parse(i['expire_time']).replace(tzinfo=bjtz)
      now = datetime.datetime.now().replace(tzinfo=sydtz)
      if time.localtime().tm_isdst:
        now = now + datetime.timedelta(hours=-1)
      secs = (exp_time - now).total_seconds()
      if secs < 10:
        print("Calculation")
        print(i)
        print(exp_time)
        print(now)
        print(secs)
        to_remove_list.append(i)

    res = db.proxy_col.delete_many({
        '_id': {
            '$in': [i['_id'] for i in to_remove_list]
        }
    })
    num = res.deleted_count
    if num != len(to_remove_list):
      import ipdb; ipdb.set_trace(context=7)

    # insert new ips
    if num:
      print("Deleted :")
      for i in to_remove_list:
        print(i)
      ip_list = get_ip_list(num)
      print("Insert " + str(ip_list))
      db.insert_proxy(ip_list)
  except:
    import ipdb; ipdb.set_trace(context=7)

if __name__ == "__main__":
  db.insert_proxy(get_ip_list(20))
  time.sleep(2)
  while True:
    update_ip_list()
    time.sleep(5)
