# -*- coding: utf-8 -*-
import re
import time
from urllib.parse import quote
import dateparser as dp
import validators


def quote_url(url):
  return quote(url, safe=';/?:@&=+$,()')


def filter_ric(string):
  '''
  filter out all white spaces (\r \t \n etc.)
  '''
  ftr = re.compile(r'[\w\d]+')
  return ftr.findall(string.strip())[0]


def filter_spaces(string):
  '''
  filter out all white spaces (\r \t \n etc.)
  keep spaces
  '''
  ftr = re.compile(r'[\S ]+')
  return ftr.findall(string.strip())


def create_date_time_tzinfo(date_str, tzinfo, date_formats: list = None):
  if not date_formats:
    date_formats = []
  date_time = dp.parse(
      date_str,
      settings={
          'TIMEZONE': tzinfo,
          'RETURN_AS_TIMEZONE_AWARE': True
      },
      date_formats=date_formats)
  return date_time


def validate_url(url):
  return validators.url(url)
