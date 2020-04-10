import scrapy
from scrapy.loader.processors import MapCompose
from datetime import datetime
from scrapy.loader.processors import TakeFirst, Join

POST_TEXT_LENGTH = 160
TEXT_PRESENTATION_DATE_FORMAT = '%b %d, %Y'
DATE_FORMAT = '%Y-%m-%d'


def _text_presentation_to_date(date_str):
    return datetime.strptime(date_str, TEXT_PRESENTATION_DATE_FORMAT)


def _date_to_str(date):
    return datetime.strftime(date, DATE_FORMAT)


class TruncatePostText(object):

    def __call__(self, values):
        full_text = " ".join(values)
        if len(full_text) > POST_TEXT_LENGTH:
            result = full_text[:POST_TEXT_LENGTH]
        else:
            result = full_text
        return result


class ArticleItem(scrapy.Item):

    title = scrapy.Field(output_processor=TakeFirst())
    url = scrapy.Field(output_processor=TakeFirst())
    authors = scrapy.Field()
    publication_date = scrapy.Field(input_processor=MapCompose(_text_presentation_to_date), output_processor=TakeFirst())
    tags = scrapy.Field()
    post = scrapy.Field(output_processor=TruncatePostText())

    def to_dict(self):
        article = self
        return dict(title=article['title'],
                    url=article['url'],
                    authors=article['authors'],
                    publication_date=_date_to_str(article['publication_date']),
                    tags=', '.join(article['tags']),
                    post=article['post'])


class AuthorItem(scrapy.Item):

    url = scrapy.Field(output_processor=TakeFirst())
    full_name = scrapy.Field(output_processor=TakeFirst())
    job_title = scrapy.Field(output_processor=TakeFirst())
    linkedin_url = scrapy.Field(output_processor=TakeFirst())

    def to_dict(self):
        return dict(full_name=self.get('full_name', ''),
                    job_title=self.get('job_title', None),
                    linkedin_url=self.get('linkedin_url', None))
