import logging
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Table, ForeignKey
from sqlalchemy import create_engine, func
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, relationship
from common import Singleton

logger = logging.getLogger(__name__)

DeclarativeBase = declarative_base()

_db_entity = None


def connect_db(database):
    global _db_entity
    _db_entity = DBEntity(database)


def close_db():
    if _db_entity:
        _db_entity.close_db()
    else:
        logging.warning('DB is not connected')


class DBEntity(metaclass=Singleton):

    def __init__(self, database):
        engine = create_engine(URL(**database))
        self._create_tables(engine)
        self.session = sessionmaker(bind=engine)()

    def _create_tables(self, engine):
        DeclarativeBase.metadata.create_all(engine, checkfirst=True)

    def close_db(self):
        self.session.close()


association_table = Table('article_author', DeclarativeBase.metadata,
                          Column('article_url', Integer, ForeignKey('article.url')),
                          Column('author_url', Integer, ForeignKey('author.url')))


class Article(DeclarativeBase):
    __tablename__ = "article"

    url = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    authors = relationship("Author", secondary=association_table)
    publication_date = Column(DateTime, nullable=False)
    tags = Column(String, nullable=False)
    post = Column(String, nullable=False)

    def __init__(self, url, title, publication_date, tags, post, authors):
        self.url = url
        self.title = title
        self.publication_date = publication_date
        self.tags = tags
        self.post = post
        self.authors = authors

    def __repr__(self):
        return "<Article({})>".format(self.url)

    def to_dict(self):
        return dict(url=self.url,
                    title=self.title,
                    publication_date=self.publication_date,
                    tags=self.tags,
                    post=self.post,
                    authors=[author.to_dict() for author in self.authors])


class Author(DeclarativeBase):
    __tablename__ = "author"

    url = Column(String, primary_key=True)
    full_name = Column(String)
    job_title = Column(String)
    linkedin_url = Column(String)

    def __init__(self, url, full_name, job_title, linkedin_url):
        self.url = url
        self.full_name = full_name
        self.job_title = job_title
        self.linkedin_url = linkedin_url

    def __repr__(self):
        return "<Author({})>".format(self.url)

    def to_dict(self):
        return dict(url=self.url, full_name=self.full_name, job_title=self.job_title, linkedin_url=self.linkedin_url)


class ArticleDao(metaclass=Singleton):

    def exists(self, url):
        q = _db_entity.session.query(Article).filter(Article.url == url)
        return _db_entity.session.query(q.exists()).scalar()

    def add(self, url, title, publication_date, tags, post, authors):
        new_article = Article(url=url,
                              title=title,
                              publication_date=publication_date,
                              tags=tags,
                              post=post,
                              authors=authors)
        _db_entity.session.add(new_article)
        _db_entity.session.commit()
        return new_article

    def all(self):
        records = _db_entity.session.query(Article).all()
        return [record.to_dict() for record in records]


class AuthorDao(metaclass=Singleton):

    def get_by_url(self, url):
        return _db_entity.session.query(Author).filter(Author.url == url).one_or_none()

    def add(self, url, full_name=None, job_title=None, linkedin_url=None):
        new_author = Author(url=url,
                            full_name=full_name,
                            job_title=job_title,
                            linkedin_url=linkedin_url)
        _db_entity.session.add(new_author)
        _db_entity.session.commit()
        return new_author

    def all_authors_with_article_count(self):
        records = _db_entity.session.query(func.count(Article.url), Author) \
            .join(Author, Article.authors).group_by(Author).all()
        result = []
        for record in records:
            author_data = record[1].to_dict()
            author_data['article_count'] = record[0]
            result.append(author_data)
        return result

    def update(self, url, full_name=None, job_title=None, linkedin_url=None):
        fields = dict(full_name=full_name,
                    job_title=job_title,
                    linkedin_url=linkedin_url)
        _db_entity.session.query(Author).filter(Author.url == url)\
            .update(fields)
        _db_entity.session.commit()

