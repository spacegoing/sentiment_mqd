# -*- coding: utf-8 -*-
import traceback
import scrapy
from GubaExchange import ExchangeParser
import Utils.GeneralUtils as utils
import Utils.DbUtils as du


class GubaSpider(scrapy.Spider):
  name = 'guba'

  def __init__(self):
    super().__init__()
    self.start_mkt_urls = [
        'http://guba.eastmoney.com/remenba.aspx?type=1&tab=1',  # shanghai
        'http://guba.eastmoney.com/remenba.aspx?type=1&tab=2',  # shenzhen
        'http://guba.eastmoney.com/remenba.aspx?type=1&tab=3',  # hongkong
        'http://guba.eastmoney.com/remenba.aspx?type=1&tab=4',  # us
    ]
    self.start_topic_urls = [
        'http://guba.eastmoney.com/remenba.aspx?type=2',  # subject forum
        'http://guba.eastmoney.com/remenba.aspx?type=3',  # industry forum
        'http://guba.eastmoney.com/remenba.aspx?type=4'  # concept forum
    ]

    self.exchange = ExchangeParser()
    # private
    # if self.exchange.is_multi_source_exchange:
    self.latest_date = utils.create_date_time_tzinfo('30 DEC 2017',
                                                     self.exchange.tzinfo)

  def start_requests(self):
    for url in self.start_mkt_urls:
      yield scrapy.Request(url, callback=self.parse_stock_urls_page)

  def parse_stock_urls_page(self, response):
    '''
    Parse page contains urls to be further scraped
    '''
    li_list = response.xpath('//ul[contains(@class,"ngblistul2")]/li')

    # todo: stock_urls_page_parser
    url_dict_list = []
    for li in li_list:
      raw_str = li.xpath('string(./a)').extract_first()
      stock_code = utils.re_code_in_parenthesis(raw_str).strip()
      stock_name = raw_str.split(')')[-1].strip()
      rel_url = li.xpath('./a/@href').extract_first()
      abs_url = response.urljoin(rel_url)
      url_dict = {
          'stock_code': stock_code,
          'stock_name': stock_name,
          'stock_url': abs_url
      }
      url_dict_list.append(url_dict)

    for i in url_dict_list:
      # yield scrapy.Request(
      #     i['stock_url'], callback=self.parse_forum_page, meta=i)
      yield scrapy.Request(
          'http://guba.eastmoney.com/list,600000.html',
          callback=self.parse_forum_page,
          meta=i)

  def parse_forum_page(self, response):
    '''
    forum page: list of posts of one topic
    '''
    # from scrapy.shell import inspect_response
    # inspect_response(response, self)

    import dateparser as dp
    # exclude top advertisement posts (class="settop" or id="ad_topic")
    post_list = response.xpath('//div[contains(@class,"articleh") and ' +
                               'not(.//em[contains(@class,"settop")]) and '
                               'not(contains(@id, "ad_topic"))]')
    post_meta_dict_list = []
    item = {}
    for i, p in enumerate(post_list):
      try:
        # l1 & l2 read, reply numbers
        read_no = p.xpath(
            'string(./span[contains(@class,"l1")])').extract_first()
        read_no = int(read_no)
        reply_no = p.xpath(
            'string(./span[contains(@class,"l2")])').extract_first()
        reply_no = int(reply_no)
        # l3 title
        label = p.xpath(
            'string(./span[contains(@class,"l3")]/em)').extract_first()
        label = label.strip()
        title = p.xpath(
            './span[contains(@class,"l3")]/a/@title').extract_first()
        title = title.strip()
        post_rel_url = p.xpath(
            './span[contains(@class,"l3")]/a/@href').extract_first()
        post_url = response.urljoin(post_rel_url)
        # add url parameters. sort comment from newest to oldest
        pos = post_url.index(".html")
        post_url = post_url[:pos] + ',d.html#storeply'
        # l4 user
        user_name = p.xpath(
            './span[contains(@class,"l4")]/a/text()').extract_first()
        user_name = user_name.strip()
        user_rel_url = p.xpath(
            './span[contains(@class,"l4")]/a/@href').extract_first()
        user_url = response.urljoin(user_rel_url)
        # l5 last comment time
        last_comment_time = p.xpath(
            './span[contains(@class,"l5")]/text()').extract_first()
        last_comment_time = dp.parse(last_comment_time.strip())
        post_meta_dict = {
            'read_no': read_no,
            'reply_no': reply_no,
            'title': title,
            'post_url': post_url,
            'label': label,
            'user_name': user_name,
            'user_url': user_url,
            'last_comment_time': last_comment_time
        }
        post_meta_dict_list.append(post_meta_dict)

      except Exception as e:  # not news row, skip
        # todo: exception handler
        item['error'] = {
            'post_row_html': p.extract(),
            'error_message': '%s: %s' % (e.__class__, str(e)),
            'row_no': i,
            'traceback': traceback.format_exc(),
            'url': response.url
        }
        print('#' * 100)
        print(item)
        print('#' * 100)

    for p_dict in post_meta_dict_list[:1]:
      p_dict.update(response.meta)
      # yield scrapy.Request(
      #     p_dict['post_url'], callback=self.parse_post_page, meta=p_dict)
      yield scrapy.Request(
          'http://guba.eastmoney.com/news,600000,750692559,d.html#storeply',
          callback=self.parse_post_page,
          meta=p_dict)

  def parse_post_page(self, response):
    '''
    post page: post itself and comments
    '''
    from scrapy.shell import inspect_response
    inspect_response(response, self)
    meta = response.meta

    import dateparser as dp
    import math

    post_time = response.xpath(
        'string(//div[contains(@class,"zwfbtime")])').extract_first()
    post_time = utils.re_datetime_in_post(post_time)
    post_time = dp.parse(post_time)
    post_content_html = response.xpath(
        '//div[contains(@id,"zwconbody")]').extract_first()

    # there are class values like zwliimage etc
    # contains zwli as sub string
    comment_list = response.xpath(
        '//div[contains(concat(" ", @class, " "), " zwli ")]')
    comment_dict_list = []
    for c in comment_list:
      # comment meta data
      comment_id = c.xpath('./@id').extract_first()
      comment_reply_id = c.xpath('./@data-huifuid').extract_first()
      comment_reply_uid = c.xpath('./@data-huifuuid').extract_first()
      comment_time = c.xpath(
          './/div[contains(@class,"zwlitime")]/text()').extract_first()
      comment_time = utils.re_datetime_in_post(comment_time)
      comment_time = dp.parse(comment_time)
      # user data
      user_name = c.xpath(
          './/span[contains(@class,"zwnick")]/a/text()').extract_first()
      user_name = user_name.strip()
      user_url = c.xpath(
          './/span[contains(@class,"zwnick")]/a/@href').extract_first()
      user_url = response.urljoin(user_url)
      # comment data
      comment_text = c.xpath(
          'string(.//div[contains(@class,"zwlitext")])').extract_first()
      comment_text = utils.filter_spaces(comment_text)[0]
      # reply to parent comment content
      comment_reply_parent_html = c.xpath(
          './/div[contains(@class,"zwlitalkbox")]').extract_first()
      comment_parent_reply_id = 0
      if not comment_reply_parent_html:
        comment_reply_parent_html = ''
      else:
        comment_parent_reply_id = c.xpath(
            './/div[contains(@class,"zwlitalkboxtext")]/@data-huifuid'
        ).extract_first()
      comment_dict = {
          "comment_id": comment_id,
          "comment_reply_id": comment_reply_id,
          "comment_reply_uid": comment_reply_uid,
          "comment_time": comment_time,
          "user_name": user_name,
          "user_url": user_url,
          "comment_text": comment_text,
          "comment_reply_parent_html": comment_reply_parent_html,
          "comment_parent_reply_id": comment_parent_reply_id
      }
      comment_dict_list.append(comment_dict)

    last_comment_time = post_time
    if comment_dict_list:
      last_comment_time = comment_dict_list[0]["comment_time"]

    post_dict = {
        "post_time": post_time,
        "post_content_html": post_content_html,
        "label": meta['label'],
        "last_comment_time": last_comment_time,
        "read_no": meta['read_no'],
        "title": meta['title'],
        "user_name": meta['user_name'],
        "user_url": meta['user_url'],
        "comment_dict_list": comment_dict_list
    }

    # pagination
    # from js file function gubanews.pager
    page_info = response.xpath(
        '//span[@id="newspage"]/@data-page').extract_first()
    _, total_num, per_page_num, cur_page = page_info.split('|')
    total_num, per_page_num, cur_page = [
        int(i.strip()) for i in [total_num, per_page_num, cur_page]
    ]
    # from js file define("guba_page", function() {
    page_num = math.ceil(total_num / per_page_num)
    pos = response.url.index(".html")
    page_url = [(response.url[:pos] + "_%d.html#storeply" % i)
                for i in range(2, page_num + 1)]
    for u in page_url:
      yield scrapy.Request(
          u, callback=self.parse_post_page, meta=meta)
    # stop_scrape_flag = False
    # news_list = self.exchange.get_news_list(response)
    # if not news_list:
    #   raise Exception('Error: Website Structure Has Been Changed!' +
    #                   ' Maintainance Needed!')
    # for i, news_row in enumerate(news_list):
    #   # has to assign new dict every loop
    #   # otherwise mongodb raises dup key (Id) error
    #   item = {
    #       'mkt': self.exchange.uptick_name,
    #       'mkt_id': self.mkt_id,
    #       'tzinfo': self.exchange.tzinfo,
    #       'error': True
    #   }
    #   try:  # news row won't have error
    #     date_time, url, title, misc_fields_dict = self.exchange.get_news_fields(
    #         news_row)

    #     # database has previous news and scraped news is older than database
    #     if self.latest_date and date_time < self.latest_date:
    #       stop_scrape_flag = True
    #       break

    #     # insert record to mongodb
    #     item['date_time'] = date_time
    #     item['title'] = title
    #     item['url'] = url
    #     item['error'] = False
    #     item.update(misc_fields_dict)
    #     yield item

    #   except Exception as e:  # not news row, skip
    #     item['error'] = {
    #         'news_row_html': news_row.extract(),
    #         'error_message': '%s: %s' % (e.__class__, str(e)),
    #         'row_no': i,
    #         'traceback': traceback.format_exc(),
    #         'url': response.url
    #     }
    #     yield item
    #     continue

    # # todo: test without keep_follow_page flag
    # if not stop_scrape_flag:
    #   for url, meta in self.exchange.get_pagination_urls(response):
    #     yield scrapy.Request(url, callback=self.parse_forum_page, meta=meta)

  # todo: multi-spider close
  def closed(self, reason):
    self.logger.info('spider closed: ' + reason)
    du.close_mongo_access()
