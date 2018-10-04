# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import Utils.DbUtils as du


class GubaSpidersPipeline(object):

  def process_item(self, item, spider):
    meta = item['meta_dict']
    if item['error']:
      du.error_insert(item, meta)
    else:
      db_handler_str = item['db_handler']
      du.db_handler_dict[db_handler_str](item['result'], meta)
    return item

  def close_spider(self, spider):
    du.client.close()
