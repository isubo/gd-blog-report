import os
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess


os.environ['SCRAPY_SETTINGS_MODULE'] = 'gdblog.settings'


def scrawl():
    process = CrawlerProcess(get_project_settings())
    process.crawl('posts-spider')
    process.start()
