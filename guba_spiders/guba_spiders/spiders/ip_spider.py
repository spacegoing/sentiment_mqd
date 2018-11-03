# -*- coding: utf-8 -*-
import scrapy
from Utils import GeneralUtils as util


class Spider(scrapy.Spider):
  name = 'ipx'
  allowed_domains = []

  def start_requests(self):

    url = 'https://www.showmyipaddress.eu/'
    for i in range(10000):
      yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

  def parse(self, response):
    self.logger.info(
        util.filter_spaces(
            response.xpath('string(//div[@class="list-group"]/div[1])')
            .extract_first()))
