import matplotlib.pyplot as plt
import pandas as pd
import nltk
from nltk.tokenize import regexp_tokenize
from nltk.corpus import stopwords
import operator
from dao.PostgresDao import PostgresDao


__all__ = ['CNNNewsNlpAnalyzer']


class CNNNewsNlpAnalyzer:

    def __init__(self):
        self.dao = PostgresDao()
        self.table_name = 'cnn_news'
        self.cleanse_list = ['desired', 'years', 'year', 'francisco', 'apply', 'writing', 'e.g', '/', '’', '-', '.',
                             ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}']
        self.tokenize_pattern = r'[a-zA-Z]\w+'
        self.primary_key = self.primary_key
        nltk.download('stopwords')

    def get_filename(self, article_df, index):
        filename = article_df.loc[index, self.primary_key] + '.txt'
        return filename

    def get_wordfrequencies_dist(self, text):
        wordsFiltered = self.filter_stopwords(text)
        freq = nltk.FreqDist(wordsFiltered)
        return freq

    def get_wordfrequencies_df(self, text):
        freq = self.get_wordfrequencies_dist(text)
        sorted_freq = sorted(
            freq.items(), key=operator.itemgetter(1), reverse=True)
        df = pd.DataFrame(sorted_freq, columns=['word', 'count'])
        selected_df = df.loc[df['count'] > 1, :]
        return selected_df

    def plot_histograms(self, freq, filename):
        fig = plt.figure()
        # selected_df.plot(x=selected_df['word'], kind='bar', figsize=(30, 10))
        freq.plot(50, cumulative=False)
        fig.savefig(filename.replace('.txt', '.png'), dpi=fig.dpi)
        # plt.show()

    def filter_stopwords(self, text):
        stop_words = set(stopwords.words('english'))
        stop_words.update(self.cleanse_list)
        nltk_tokens = regexp_tokenize(text, self.tokenize_pattern)
        wordsFiltered = []
        for w in nltk_tokens:
            if w.lower() not in stop_words:
                wordsFiltered.append(w.lower())
        return wordsFiltered

    def process_article_insights(self):
        print("Starting method process_article_insights...")
        postgres_df = self.dao.read_table(self.table_name)
        for i in range(0, len(postgres_df.loc[:, self.primary_key])):
            filename = self.get_filename(postgres_df, i)
            print("Processing file {0}".format(filename))
            with open(filename, 'r') as reader:
                text = reader.read()
            print("Processing word frequencies for file {0}".format(filename))
            selected_df, freq = self.get_wordfrequencies_df(text)
            print("Creating plot for file {0}".format(filename))
            self.plot_histograms(freq, filename)
            print("Writes word frequencies csv for file {0}".format(filename))
            selected_df.to_csv(filename.replace(
                '.txt', '.csv'), sep=',', encoding='utf-8')
        print("Done method process_article_insights.")


def run(self):
    self.process_article_insights()


if __name__ == '__main__':
    run()
