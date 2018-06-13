import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import nltk
import newspaper
import time
from nltk.tokenize import sent_tokenize, word_tokenize, regexp_tokenize
from nltk.corpus import stopwords
import operator
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
        nltk.download('stopwords')

    def get_filename(self, article_df, index):
        filename = article_df.loc[index, 'article_id'] + '.txt'
        return filename

    def get_wordfrequencies_dist(self, text):
        wordsFiltered = self.filterStopwords(text)
        freq = nltk.FreqDist(wordsFiltered)
        return freq

    def get_wordfrequencies_df(self, text):
        freq = self.get_wordfrequencies_dist(text)
        sorted_freq = sorted(
            freq.items(), key=operator.itemgetter(1), reverse=True)
        df = pd.DataFrame(sorted_freq, columns=['word', 'count'])
        selected_df = df.loc[df['count'] > 1, :]
        return selected_df

    def plotHistograms(self, freq, filename):
        fig = plt.figure()
        # selected_df.plot(x=selected_df['word'], kind='bar', figsize=(30, 10))
        freq.plot(50, cumulative=False)
        fig.savefig(filename.replace('.txt', '.png'), dpi=fig.dpi)
        # plt.show()

    def filterStopwords(self, text):
        stop_words = set(stopwords.words('english'))
        stop_words.update(['desired', 'years', 'year', 'francisco', 'apply', 'writing', 'e.g', '/', '’', '-', '.',
                           ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}'])  # remove it if you need punctuation
        pattern = r'[a-zA-Z]\w+'
        nltk_tokens = regexp_tokenize(text, pattern)
        wordsFiltered = []
        for w in nltk_tokens:
            if w.lower() not in stop_words:
                wordsFiltered.append(w.lower())
        return wordsFiltered

    def process_article_insights(self):
        postgres_df = self.dao.read_table(self.table_name)
        for i in range(0, len(postgres_df.loc[:, 'article_id'])):
            filename = self.get_filename(postgres_df, i)
            with open(filename, 'r') as reader:
                text = reader.read()
            selected_df, freq = self.get_wordfrequencies_df(text)
            self.plotHistograms(freq, filename)
            selected_df.to_csv(filename.replace(
                '.txt', '.csv'), sep=',', encoding='utf-8')

    # def retrieveTextBody(self):

    #     return text

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
        for i in range(0, len(article_df.loc[:, 'article_id'])):
            # filename = article_df.loc[i, 'article_id'] + '.txt'
            filename = self.get_filename(article_df, i)
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
