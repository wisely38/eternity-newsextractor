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
        self.table_parameters = ['cnn_news', 'article_id',
                                 'category', 'url', 'authors', 'keywords']
        self.cnn_paper = newspaper.build(self.cnn_url)
        self.article_df = pd.DataFrame(columns=self.columns)
        nltk.download('punkt')

    def get_filename(self, article_df, index):
        filename = article_df.loc[index, 'article_id'] + '.txt'
        return filename

    def extract_site_metadata(self):
        print("Starting method extract_site_metadata...")
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
            print("Creates new table...")
            self.dao.create_table(self.table_parameters)
            print("Writes to new table...")
            self.write_to_db(article_df)
        elif self.dao.has_records(
                self.table_name, self.columns[0],
                article_df.loc[:, ['article_id']].values.tolist()):
            print("Read table from DB...")
            existing_postgres_df = self.dao.read_table('cnn_news')
            combined_postgres_df = pd.concat(
                [self.article_df, existing_postgres_df])
            combined_postgres_df.drop_duplicates(
                subset='article_id', keep='first', inplace=True)
            print("Delete whole table from DB...")
            self.dao.delete_table('cnn_news')
            print("Recreate whole table to DB...")
            self.dao.create_table(self.table_parameters)
            self.write_to_db(combined_postgres_df)
        print("Done method extract_site_metadata.")

    def write_to_file(self, article_df):
        print("Starting method write_to_file...")
        for i in range(0, len(article_df.loc[:, 'article_id'])):
            # filename = article_df.loc[i, 'article_id'] + '.txt'
            filename = self.get_filename(article_df, i)
            with open(filename, 'w') as output:
                print('Writing out file: ', filename)
                output.writelines(article_df.loc[i, 'body'])
        print("Done method write_to_file...")

    def write_to_db(self, article_df):
        print("Starting method write_to_db...")
        article_postgres_df = article_df.drop('body', 1)
        self.dao.write_table(article_postgres_df, self.table_parameters)
        print("Done method write_to_db...")


def run(self):
    self.extract_site_metadata()


if __name__ == '__main__':
    run()
