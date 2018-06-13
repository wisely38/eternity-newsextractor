import pandas as pd
import nltk
import newspaper
import time
from dao.PostgresDao import PostgresDao


__all__ = ['CNNNewsExtractor']


class CNNNewsExtractor:

    def __init__(self):
        self.cnn_url = 'http://cnn.com'
        self.dao = PostgresDao()
        self.table_name = 'cnn_news'
        self.columns = ['article_id', 'category',
                        'url', 'authors', 'keywords', 'body']
        self.cnn_paper = newspaper.build(self.cnn_url)
        self.article_df = pd.DataFrame(columns=self.columns)
        nltk.download('punkt')

    def extract_site_metadata(self):
        for article in self.cnn_paper.articles:
            article.download()
            time.sleep(2)
            article.parse()
            article.nlp()
            str_arr = article.url.split('/')
            if len(str_arr) > 6:
                self.article_df.loc[-1, 'article_id'] = str_arr[6] + \
                    '-' + str(hash(article.url))
                self.article_df.loc[-1, 'category'] = str_arr[6]
            else:
                print(article.url)
                print(str_arr)
                break
            self.article_df.loc[-1, 'url'] = article.url
            self.article_df.loc[-1, 'authors'] = ",".join(article.authors)
            self.article_df.loc[-1, 'keywords'] = ",".join(article.keywords)
            self.article_df.loc[-1, 'body'] = article.text
            self.article_df.index = self.article_df.index + 1
            self.article_df.sort_index()
        self.article_df.drop_duplicates(
            subset='body', keep='first', inplace=True)
        article_df = self.article_df.reset_index(drop=True)
        self.write_to_file(article_df)

        # check if record already exists
        if not self.dao.has_table(self.table_name):
            self.dao.create_table(
                ['cnn_news', 'article_id', 'category', 'url', 'authors', 'keywords'])
            self.write_to_db(article_df)
        elif self.dao.has_records(
                self.table_name, self.columns[0], article_df.loc[:, ['article_id']].values.tolist()):
            existing_postgres_df = self.dao.read_table('cnn_news')
            combined_postgres_df = pd.concat(
                [self.article_df, existing_postgres_df])
            combined_postgres_df.drop_duplicates(
                subset='article_id', keep='first', inplace=True)
            self.dao.delete_table('cnn_news')
            self.dao.create_table(
                ['cnn_news', 'article_id', 'category', 'url', 'authors', 'keywords'])
            self.write_to_db(combined_postgres_df)

    def write_to_file(self, article_df):
        for i in range(1, len(article_df.loc[:, 'article_id'])):
            filename = article_df.loc[i, 'article_id'] + '.txt'
            with open(filename, 'w') as output:
                print('Writing out file: ', filename)
                output.writelines(article_df.loc[i, 'body'])

    def write_to_db(self, article_df):
        article_postgres_df = article_df.drop('body', 1)
        self.dao.write_table(article_postgres_df,
                             ['cnn_news', 'article_id', 'category', 'url', 'authors', 'keywords'])


def run(self):
    self.extract_site_metadata()


if __name__ == '__main__':
    run()
