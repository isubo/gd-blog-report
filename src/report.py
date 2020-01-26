import os
import dao
from dao import AuthorDao, ArticleDao
import pandas as pd
import matplotlib.pyplot as plt
from shutil import copyfile


class ReportGenerator:

    template_path = "./resources/reportTemplate.html"
    css_path = "./resources/report.css"
    result_folder = "./report"

    def __init__(self):
        self._author_dao = AuthorDao()
        self._article_dao = ArticleDao()

    def generate(self):
        dao.connect_db({'drivername': 'sqlite', 'database': 'articles.db'})
        try:
            os.makedirs(self.result_folder, exist_ok=True)
            copyfile(self.css_path, os.path.join(self.result_folder, 'report.css'))

            with open(self.template_path) as fin:
                template = fin.read()
            content = template.format(top_5_authors=self.get_top_5_authors_html(),
                                      top_5_new_articles=self.get_top_5_articles_html())
            with open(os.path.join(self.result_folder, 'report.html'), 'w') as fout:
                fout.write(content)

            self.top_tags_plot()
        finally:
            dao.close_db()

    def get_top_5_authors_html(self):
        row_template = """
        <div class="row">
            <div class='fullName'>{full_name}</div><div>{articles}</div>
        </div>
        """
        html = ""
        for index, row in self.get_top_5_authors().iterrows():
            html += row_template.format(full_name=row['full_name'], articles=row['article_count'])
        return html

    def get_top_5_authors(self):
        all_authors = self._author_dao.all_authors_with_article_count()
        authors_df = pd.DataFrame(all_authors)
        authors_df.sort_values(by=['article_count'], ascending=False, inplace=True)
        top_5 = authors_df.head(5)
        return top_5

    def get_top_5_articles_html(self):
        row_template = """
        <div>
            <div><b>{publication_date}</b></div><div>{title}</div>
        </div>
        """
        html = ""
        for index, row in self.get_top_5_articles().iterrows():
            html += row_template.format(title=row['title'], publication_date=row['publication_date'])
        return html

    def get_top_5_articles(self):
        articles = self._article_dao.all()
        articles_df = pd.DataFrame(articles)
        articles_df.sort_values(by=['publication_date'], ascending=False, inplace=True)
        top_5 = articles_df.head(5)
        return top_5

    def count_tags(self):
        articles = self._article_dao.all()
        articles_df = pd.DataFrame(articles)
        tags_map = {}
        for tags_row in articles_df.loc[:, 'tags']:
            for tag in tags_row.split(','):
                tags_map[tag] = tags_map[tag] + 1 if tags_map.get(tag) else 1
        tags_array = [dict(key=key, value=value) for key, value in tags_map.items()]
        return sorted(tags_array, key=lambda tag : tag['value'])[0:7]

    def top_tags_plot(self):

        data = self.count_tags()
        tags = [item['key'] for item in data]
        values = [item['value'] for item in data]

        plt.rcdefaults()
        fig, ax = plt.subplots()

        ax.barh(range(len(data)), values, align='center', tick_label=tags)
        ax.set_yticks(range(len(tags)))
        ax.set_yticklabels(tags)
        ax.set_xlabel('Count')
        ax.set_title('7 popular tags')

        fig.set_size_inches(10, 5)

        plt.savefig(os.path.join(self.result_folder, 'topTags.png'))
