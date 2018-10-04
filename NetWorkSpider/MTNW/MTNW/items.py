# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ChapterItem(scrapy.Item):
    # define the fields for your item here like:
    chapter_name = scrapy.Field()
    chapter_url = scrapy.Field()
    num = scrapy.Field()  # 用于绑定章节顺序


class DocumentItem(scrapy.Item):
    content = scrapy.Field()  # 章节内容
    chapter_name = scrapy.Field()
