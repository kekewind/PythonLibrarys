# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import redis


class MtnwPipeline(object):
    def __init__(self):
        self.rdb = redis.Redis(host="localhost", port=6379, db=0)

    def process_item(self, item, spider):
        self.rdb.set(str(item['chapter_name']), str(item['chapter_name'] + "\n" + item['content']))
        return item
