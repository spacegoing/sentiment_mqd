# -*- coding: utf-8 -*-
import Utils.GeneralUtils as utils


class ExchangeParser:
  # market meta config
  uptick_name = 'guba'
  tzinfo = 'Asia/Shanghai'  # no shenzhen timezone
  # web config
  # website_url = 'http://guba.eastmoney.com/remenba.aspx?type=1'
  website_url = 'http://guba.eastmoney.com/topic,600010.html'
  root_url = 'http://guba.eastmoney.com'
  # todo: if get_pagination_ruls yield none
  # we need this
  # we can not always validate url. sometimes it will be
  # urljoin() therefore it is always valid
  keep_follow_pagination = False
  # db config
  col_name = 'paris'
  is_multi_source_exchange = False

  # private
  page_no = 1
  pagination_template = 'https://www.euronext.com/en/news/?page=%d'

  def get_start_urls(self, **parameters):
    yield self.website_url

  def get_pagination_urls(self, response):
    meta = dict()
    rel_url = response.xpath(
        './/li[contains(@class,"pager-next")]//a/@href').extract_first()
    url = response.urljoin(rel_url)
    if utils.validate_url(url):
      yield url, meta

  def get_news_list(self, response):
    import ipdb; ipdb.set_trace(context=7)
    news_list = response.xpath(
        '//table[contains(@class,"views-table")]/tbody/tr[contains(@class,"odd") or contains(@class, "even")]'
    )
    if not news_list:
      news_list = response.xpath(
          '//table[contains(@class,"views-table")]/tr[contains(@class,"odd") or contains(@class, "even")]'
      )
    return news_list

  def get_news_fields(self, news_row):
    misc_fields_dict = dict()
    date_time = self.get_date_time(news_row)
    url = self.get_url(news_row)
    title = self.get_title(news_row)
    # customized code
    # if exchange has multi news sources
    # assign key 'website_url' to misc_fields_dict
    # misc_fields_dict['website_url'] = self.website_url
    return date_time, url, title, misc_fields_dict

  def get_date_time(self, news_row):
    date_str = news_row.xpath(
        'string(.//span[contains(@class, "date-display-single")])'
    ).extract_first().strip()
    if date_str:
      date_time = utils.create_date_time_tzinfo(date_str, self.tzinfo)
      return date_time
    else:
      raise Exception('Error: Date parsing error')

  def get_url(self, news_row):
    url = news_row.xpath(
        './/td[contains(@class, "views-field-field-productnews-display-title-value")]/a/@href'
    ).extract_first().strip()
    url = self.root_url + url
    return utils.quote_url(url)

  def get_title(self, news_row):
    title = news_row.xpath(
        'string(.//td[contains(@class, "views-field-field-productnews-display-title-value")]/a)'
    ).extract_first().strip()
    return title

  # below write customized methods
