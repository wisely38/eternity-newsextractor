import pandas as pd
import newspaper
import time
from dao.PostgresDao import PostgresDao


class CNNNewsExtractor:

    def __init__(self):
        self.cnn_url = 'http://cnn.com'
        self.dao = PostgresDao()

    def extract_site_metadata(self):
        cnn_paper = newspaper.build(self.cnn_url)
        columns = ['article_id', 'category',
                   'url', 'authors', 'keywords', 'body']
        article_df = pd.DataFrame(columns=columns)
        for article in cnn_paper.articles:
            article.download()
            time.sleep(2)
            article.parse()
            article.nlp()
            str_arr = article.url.split('/')
            if len(str_arr) > 6:
                article_df.loc[-1, 'article_id'] = str_arr[6] + \
                    '-' + str(hash(article.url))
                article_df.loc[-1, 'category'] = str_arr[6]
            else:
                print(article.url)
                print(str_arr)
                break
            article_df.loc[-1, 'url'] = article.url
            article_df.loc[-1, 'authors'] = ",".join(article.authors)
            article_df.loc[-1, 'keywords'] = ",".join(article.keywords)
            article_df.loc[-1, 'body'] = article.text
            article_df.index = article_df.index + 1
            article_df.sort_index()
        article_df.drop_duplicates(subset='body', keep='first', inplace=True)
        article_df = article_df.reset_index(drop=True)

    def write_to_file(self, article_df):
        for i in range(1, len(article_df.loc[:, 'article_id'])):
            filename = article_df.loc[i, 'article_id'] + '.txt'
        with open(filename, 'w') as output:
            print('writing out ', filename)
            output.writelines(article_df.loc[i, 'body'])

    def write_to_db(self, article_df):
        article_postgres_df = article_df.drop('body', 1)
        self.dao.create_table(
            ['cnn_news', 'article_id', 'category', 'urls', 'authors', 'keywords'])
