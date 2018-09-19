# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import Utils.DbUtils as du


class MarketEventsSpidersPipeline(object):

  def process_item(self, item, spider):
    col_name = spider.exchange.col_name
    if item['error']:
      du.mkt_db[col_name + '_error_urls'].insert_one(item)
    else:
      du.mkt_db[col_name].insert_one(item)
    return item

  def close_spider(self, spider):
    du.client.close()
