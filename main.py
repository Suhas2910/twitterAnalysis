"""
main.py

This Python module reads the tweets, analysis them, and then stores them in PySpark.sql.DataFrame with respect to two
keywords. These keywords are provided by the user while running the script. The script analyses the follower count,
number of words, favorite count, and engagement rate of tweets. This module can be executed as follows,

$ python main.py keyword1 keyword2

where the keyword1 and keyword2 represents the first keyword and second keyword respectfully. These parameters can be
provided by the user. This module outputs plots of sentiment score, engagement rate, engagment count and the words
lengths.

NOTE: Make sure add user's credentials in the config/config.ini file before running the module.
"""

import sys

from analysis.analysis import EngagementRateAnalysis
from analysis.sentiment_analysis import SentimentAnalysis
from dependencies.twitter_api import TwitterAPI
from dependencies.spark_setup import start_spark
from tasks.tasks import *

KEYWORD1 = None
KEYWORD2 = None


def display_dataframe(df, n=10):
    print(df.show(n=n, truncate=False))


class TwitterAnalysis:
    """Creating the dataframe for tweets.
    """

    def __init__(self):
        # get the SparkSession and twitterAPI objects
        self.spark = start_spark()
        self.twitter_api = TwitterAPI()

    def create_dataframe(self, query):
        """Creates a PySpark dataframe

        create_dataframe function reads the twitter data with respect
        to query and stores the required information into PySpark DataFrame.
        :param query: For querying the tweets, which consists of keywords
        :return: PySpark DataFrame
        """
        tweets_list = []
        tweets = self.twitter_api.read_tweets(query)  # Read the tweets

        try:
            for tweet in tweets:
                # Checks if tweets have 140+ characters
                if 'extended_tweet' in tweet._json:
                    full_text = tweet._json['extended_tweet']['full_text']  # Get the full text
                else:
                    full_text = tweet.full_text
                if KEYWORD1 in full_text.lower():
                    tweets_list.append((
                        tweet.created_at, tweet.user.screen_name, tweet.user.followers_count,
                        tweet.id, tweet.retweet_count, tweet.favorite_count, full_text,
                        len(tweet._json['entities']['hashtags']), KEYWORD1
                    ))
                elif KEYWORD2 in full_text.lower():
                    tweets_list.append((
                        tweet.created_at, tweet.user.screen_name, tweet.user.followers_count,
                        tweet.id, tweet.retweet_count, tweet.favorite_count,
                        full_text, len(tweet._json['entities']['hashtags']), KEYWORD2
                    ))
                else:
                    pass
            print(f"\nCompleted reading {len(tweets_list)} tweets.")
            df = self.spark.createDataFrame(data=tweets_list, schema=get_schema())  # Create the dataframe
            df = pre_process_text(df)
            return df
        except Exception as args:
            raise f"Exception while create dataframe. ERROR:{args}"


# Entry point to this module
if __name__ == '__main__':

    args = sys.argv  # Reading the arguments

    # Checks for the number of arguments
    if len(args) != 3:
        print("Provide two keywords as parameters")
        sys.exit("Check the parameters")

    KEYWORD1 = args[1].lower()  # First argument as Keyword1
    KEYWORD2 = args[2].lower()  # Second argument as Keyword2

    # The query shows that, the tweets needs to have either keyword1 or keyword2,
    # and it does not consider the retweets
    query = f"{KEYWORD1} OR {KEYWORD2} -is:retweet"

    print(f"\nThe query for extracting tweets is: {query}")

    # Get the twitter data
    tweet_analysis = TwitterAnalysis()
    tweets_df = tweet_analysis.create_dataframe(query)

    print(f"The shape of dataframe: ({tweets_df.count()}, {len(tweets_df.columns)})")

    # Count the words from tweet.
    # tweets_df = tweet_lenth(tweets_df)

    # Get tweet with most number of favorites
    most_favorite_tweet(tweets_df)

    # Get account screen name with most number of followers
    most_followed_account(tweets_df)

    # Get average, minimum, and maximum tweet length in terms of words
    avg_max_min_tweet_length(tweets_df)

    # Calculate engagement rate
    tweets_df = calculate_eng_rate(tweets_df)
    # Calculate the average engagement rate per day
    tweets_df = average_engagement_rate_daily(tweets_df)

    # Create object of engagement rate analysis
    engagement_analysis = EngagementRateAnalysis(tweets_df, KEYWORD1, KEYWORD2)
    # Generate engagement rate period plots
    engagement_analysis.create_eng_period_chart()
    # Generate engagement rate period plots. Parameters: (KEYWORD1, KEYWORD2, 'both')
    engagement_analysis.create_daily_chart()

    # Create object of sentiment analysis
    sentiment_analasys = SentimentAnalysis(tweets_df)
    # Calculate the sentiment score of tweet
    tweets_df = sentiment_analasys.calculate_sentiment_score()
    # Generate sentiment score plot
    sentiment_analasys.plot_sentiment_score()

    display_dataframe(tweets_df)

    print("SUCCESSFUL!!!")
