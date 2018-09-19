# -*- coding: utf-8 -*-
import os
import re
import subprocess
import time
import urllib.request
from urllib.parse import quote
import dateparser as dp
import validators

# todo: configurations
PDF_DIR = '/Users/spacegoing/macCodeLab-MBP2015/MQD/Automation/Event_Collection/Market_Event_Spiders/PDFs/'

# headless chrome command
chrome_path = r'/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome'
chrome_options = ' --headless --disable-gpu --print-to-pdf=%s'

# todo: urllib user agent headers
opener = urllib.request.build_opener()
opener.addheaders = [(
    'User-agent',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
)]
urllib.request.install_opener(opener)


def create_pdf_dir(pdf_dir):
  if not os.path.exists(pdf_dir):
    os.makedirs(pdf_dir)


def save_pdf_chrome(url, dir_filename):
  cmd = chrome_path + chrome_options + ' %s'
  subprocess.call(cmd % (dir_filename, url), shell=True, timeout=60)


def quote_url(url):
  return quote(url, safe=';/?:@&=+$,()')


def save_pdf_url(url, dir_filename):
  remaining_download_tries = 5
  while remaining_download_tries > 0:
    try:
      urllib.request.urlretrieve(url, dir_filename)
      break
    except:
      time.sleep(0.1)
      print("error downloading " + url + " on trial no: " +
            str(5 - remaining_download_tries))
      remaining_download_tries = remaining_download_tries - 1
      continue
  if remaining_download_tries == 0:
    raise Exception('URL Download Error: %s' % url)


def save_pdf_url_or_chrome(url, dir_filename):
  # save PDFs
  if url.lower().endswith('.pdf'):
    save_pdf_url(url, dir_filename)
  elif url.lower().endswith(('.doc', '.docx', '.xls', '.xlsx', '.ppt',
                             '.pptx')):
    save_pdf_url(url, dir_filename)
  else:
    save_pdf_chrome(url, dir_filename)


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
