import scrapy
from scrapy.spiders import CrawlSpider
from gdblog.items import ArticleItem, AuthorItem
from scrapy.loader import ItemLoader
from dao import ArticleDao


class GDPostsCrawl(CrawlSpider):

    name = 'posts-spider'
    host = 'https://blog.griddynamics.com'
    allowed_domains = ['blog.griddynamics.com']
    articles_list_page = 'https://blog.griddynamics.com/explore'

    def __init__(self, *a, **kw):
        super(GDPostsCrawl, self).__init__(*a, **kw)
        self._article_dao = ArticleDao()

    def start_requests(self):
        yield scrapy.Request(url=self.articles_list_page, callback=self.parse_article_list)

    def parse_article_list(self, response):
        latest_article_link = \
            response.css('div#wrap div.blog div.explor:first-of-type div.cntt>h4>a::attr(href)').extract_first()
        if not self._is_data_up_to_date(latest_article_link):
            for article_url in response.css('div#wrap div.blog div.explor div.cntt h4>a::attr(href)').extract():
                yield scrapy.Request(self.host + article_url, callback=self.parse_article_details)
        else:
            self.logger.info('---- [ No new post was found ] ----')

    def _is_data_up_to_date(self, latest_article_link):
        return self._article_dao.exists(latest_article_link)

    def parse_article_details(self, response):
        loader = ItemLoader(ArticleItem(), selector=response.css('div#postcontent'))
        loader.add_value('url', self._get_page_path(response.url))
        loader.add_css('title', 'h1[itemprop=headline]::text')
        loader.add_css('publication_date', 'meta[itemprop=datePublished]::attr(content)')
        loader.add_css('tags', 'div.posttag.nomobile > a.tag::text')
        loader.add_css('post', 'div#mypost div *::text')
        loader.add_css('authors', 'div#postcontent div.postauthor a.goauthor::attr(href)')
        article = loader.load_item()

        for author_url in article['authors']:
            yield scrapy.Request(self.host + author_url, self.parse_author)

        yield article

    def parse_author(self, response):
        loader = ItemLoader(AuthorItem(), selector=response.css('div#authorbox>div.nomobile'))
        loader.add_value('url', self._get_page_path(response.url))
        loader.add_css('full_name', 'h1::text')
        loader.add_css('job_title', 'h2::text')
        loader.add_css('linkedin_url', 'div.authorsocial a.linkedin::attr(href)')
        yield loader.load_item()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CrawlSpider, cls).from_crawler(crawler, *args, **kwargs)
        spider._db_path = crawler.settings.get('DATABASE')
        return spider

    def _get_page_path(self, full_url):
        return full_url.replace(self.host, "")
