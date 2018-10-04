# -*- coding: utf-8 -*-
import scrapy
from MTNW.items import DocumentItem
from scrapy import Request


class BookDownSpider(scrapy.Spider):
    name = 'book_down'
    allowed_domains = ['www.qu.la']
    start_urls = ['https://www.qu.la/book/85467/']

    def __init__(self, name=None, **kwargs):
        super().__init__(name=None, **kwargs)
        self.queue = None

    def parse(self, response):
        urls = response.xpath('//div[@id="list"]/dl/dd/a/@href').extract()
        books = response.xpath('//div[@id="list"]/dl/dd/a/text()').extract()
        self.queue = books
        jobs = map(lambda x: {
            'url': 'https://www.qu.la' + urls[x] if urls[x].__contains__('/book/85467/') else self.start_urls[0] + urls[
                x],
            'name': books[x]}, range(len(urls)))
        for job in jobs:
            yield Request(job['url'], callback=self.parse_chapter, meta={'name': job['name']})

    def parse_chapter(self, response):
        content = response.xpath('//div[@id="content"]/text()').extract()
        if len(content) > 0:
            texts = ''.join(content).replace('\r', '').replace('\n', '').replace('\t', '').replace(u'\u3000', u'').replace(u'\xa0', u'')
            text = self.text_modify(texts)
            chapter_item = DocumentItem()
            chapter_item['content'] = text
            chapter_item['chapter_name'] = response.meta['name']
            yield chapter_item

    @staticmethod
    def text_modify(texts):
        text = ""
        for each in texts:
            if str(each).__contains__("br"):
                continue
            elif str(each).__contains__("h5"):
                continue
            elif str(each).__contains__("h4"):
                continue
            elif str(each).__contains__("h3"):
                continue
            elif str(each).__contains__("ul"):
                continue
            elif str(each).__contains__("span"):
                continue
            elif str(each).__contains__("<b>"):
                continue
            elif str(each).__contains__("li"):
                continue
            else:
                text = text + str(each).replace('\xa0', '  ').replace(
                    "（乡/\村/\小/\说/\网 wｗw.xiａngcｕnxiaｏsｈuo.cｏm）", "")
            del each
        del texts
        return text
