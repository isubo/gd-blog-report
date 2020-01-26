import logging
from gdblog.items import ArticleItem, AuthorItem
from scrapy import signals
import dao

logger = logging.getLogger(__name__)


class SqlitePipeline(object):

    def __init__(self, settings):
        self.database = settings.get('DATABASE')
        self._article_dao = dao.ArticleDao()
        self._author_dao = dao.AuthorDao()

    def process_item(self, item, spider):
        if isinstance(item, ArticleItem):
           self.process_article_item(item)
        elif isinstance(item, AuthorItem):
            self.process_author_item(item)

    def process_article_item(self, item):
        if self._article_dao.exists(item['url']):
            return
        authors = []
        for author_url in item['authors']:
            author = self._author_dao.get_by_url(author_url)
            if not author:
                author = self._author_dao.add(url=author_url)
            authors.append(author)
        self._article_dao.add(url=item['url'],
                              title=item['title'],
                              publication_date=item['publication_date'],
                              tags=','.join(item['tags']),
                              post=item['post'],
                              authors=authors)


    def process_author_item(self, item):
        author = self._author_dao.get_by_url(item['url'])
        if not author:
            self._author_dao.add(url=item['url'],
                                 full_name=item['full_name'],
                                 job_title=item.get('job_title'),
                                 linkedin_url=item.get('linkedin_url'))
        if not author.full_name:
            self._author_dao.update(url=item['url'],
                                    full_name=item['full_name'],
                                    job_title=item.get('job_title'),
                                    linkedin_url=item.get('linkedin_url'))


    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        dao.connect_db(self.database)

    def spider_closed(self, spider):
        dao.close_db()
