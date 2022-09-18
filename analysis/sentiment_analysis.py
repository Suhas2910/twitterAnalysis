import matplotlib.pyplot as plt
import numpy as np
import nltk.sentiment.vader as vd

from nltk.corpus import stopwords as sw
from nltk.stem import PorterStemmer
from nltk import download
from nltk.tokenize import word_tokenize
from pyspark.sql import functions as F
from pyspark.sql.types import *

download('vader_lexicon')

stop_words = sw.words('english')
ps = PorterStemmer()
sia = vd.SentimentIntensityAnalyzer()


def remove_stopwords(text):
    """Removes the stopword.

    :param text: Tweet text
    :return: list of words without stopwords
    """
    words_list = [str(i) for i in text.split() if not i in stop_words]
    return words_list


def stem_words(words):
    """Converts words into word stem.

    :param words: list of words without stopwords
    :return: list of stemmed words
    """
    stem_words = [str(ps.stem(i)) for i in words if i != '']
    return stem_words


def sentiment_score(words):
    """Calculates sentiment score.

    :param words: list of stemmed words
    :return: similarity score of list of words
    """
    score = round(sum([sia.polarity_scores(i)['compound'] for i in word_tokenize(' '.join(words))]), 4)
    return score


class SentimentAnalysis:
    """Calculating sentiment score.

    :param df: PySpark DataFrame
    """
    def __init__(self, df):
        # Considers the PySpark dataframe containing tweet data
        self.df = df

    def calculate_sentiment_score(self):
        """Calculates the sentiment score.

        calculate_sentiment_score defines the Spark UDFs
        and applies them to calculate the similarity score.
        :return: PySpark DataFrame containing similarity score.
        """
        remove_stopwords_udf = F.udf(lambda x: remove_stopwords(x))    # Creates spark udf
        word_stemming_udf = F.udf(lambda x: stem_words(x))    # Creates spark udf
        sentiment_score_udf = F.udf(lambda x: sentiment_score(x), DoubleType())    # Creates spark udf
        try:
            self.df = self.df.withColumn('text_without_stopwords',   # Text without stopwords
                                         remove_stopwords_udf(F.col('cleaned_text'))
                                         )
            self.df = self.df.withColumn('stem_text',    # Text after stemming
                                         word_stemming_udf(F.col('text_without_stopwords'))
                                         )
            self.df = self.df.withColumn('sentiment_score',    # Sentiment score of text
                                         sentiment_score_udf(F.col('stem_text'))
                                         )
            self.df = self.df.drop('text_without_stopwords', 'stem_text')    # Drop the unwanted columns

            print("\nCreated sentiment_score column.")

            return self.df
        except Exception as args:
            raise f"Error while calculating the sentiment score. \n ERROR: {args}"

    def plot_sentiment_score(self):
        """Generate bar plot of sentiment score.
        :return: None
        """
        try:
            # Filtering out the rows having zero similarity score
            df = self.df.filter(
                F.col('sentiment_score') != 0.0
            ).orderBy(
                F.col('created_day')
            ).toPandas()

            # Generates the positive-negative bar chart
            plt.figure(figsize=(20, 10))
            plt.bar(
                df.created_day,
                df.sentiment_score,
                color=np.where(df['sentiment_score'] < 0, 'red', 'green')
            )
            plt.title("Sentiment score plot.")
            plt.xlabel("Created Day")
            plt.ylabel("Sentiment Score")
            plt.savefig("sentiment_score_plot.png")

            print("\nCreated sentiment score plot.")

            return None
        except Exception as args:
            raise f"Error while plotting sentiment plot. ERROR: {args}"
