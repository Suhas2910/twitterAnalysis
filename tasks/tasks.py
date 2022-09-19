from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from matplotlib import pyplot as plt


def average_engagement_rate_daily(df):
    """Calculates average engagement rate daily.

    average_engagement_rate_daily function creates a new column 'daily_avg_engg_rate'.
    The function calculates the every day average by using moving window and stores the
    values in the 'daily_avg_engg_rate' column.
    :param df: PySpark dataframe
    :return: PySpark dataframe with 'daily_avg_engg_rate' column
    """
    try:
        windowSpec = Window.partitionBy(
            F.col('created_day')
        ).orderBy(
            F.col('created_at')
        )    # Creates a window by partitioning the data by day and sorting the data by tweet created date

        df = df.withColumn(
            'daily_avg_engg_rate',
            F.avg(
                F.col('engagement_rate')
            ).over(windowSpec)
        )    # Calculates the average engagement rate with respect to window

        print("\nCreated daily_avg_engg_rate column.")

        return df

    except Exception as args:
        raise f"Error in function average_engagement_rate_daily. ERROR: \n{args}"


def avg_max_min_tweet_length(df):
    """Calculates average, minimum, and maximum word count.

    :param df: PySpark DataFrame
    :return: None
    """
    df = tweet_length(df)
    try:
        aggregated_data = df.agg(
            F.round(F.mean(F.col('tweet_length')), 4).alias('avg_tweet_length'),
            F.max(F.col('tweet_length')).alias('max_tweet_length'),
            F.min(F.col('tweet_length')).alias('min_tweet_length')
        )

        print("\nAverage, Maximum, and Minimum tweet length:")
        aggregated_data.show(truncate=False)

    except Exception as args:
        raise f"Error in function avg_max_min_tweet_length. ERROR: \n{args}"


def calculate_eng_rate(df):
    """Calculate the engagement rate.

    calculate_eng_rate function calculates the engagement count by adding
    retweet count, favorite count, and hashtag count of a tweet. This function
    calculates the error rate as ERROR RATE = engagement_count/followers count.
    :param df: PySpark DataFrame
    :return: PySpark DataFrame containing engagement_count and engagement_rate columns
    """
    try:
        df = df.withColumn('engagement_count',
                           F.col('retweet_count')
                           + F.col('favorite_count')
                           + F.col('hashtag_count')
                           )
        print("Created engagement_count column.")
        df = df.withColumn('engagement_rate',
                           F.round(
                               F.col('engagement_count')
                               / F.col('followers_count')
                           )
                           )

        print("\nCreated engagement_rate column.")

        return df

    except Exception as args:
        raise f"Error in function calculate_eng_rate. ERROR: \n{args}"


def get_schema():
    """Creates schema for PySpark DataFrame.

    :return: StructType object having schema for PySpark DataFrame
    """
    try:
        tweet_schema = StructType([
            StructField('created_at', TimestampType(), True),
            StructField('screen_name', StringType(), True),
            StructField('followers_count', IntegerType(), True),
            StructField('tweet_id', LongType(), True),
            StructField('retweet_count', IntegerType(), True),
            StructField('favorite_count', IntegerType(), True),
            StructField('text', StringType(), True),
            StructField('hashtag_count', IntegerType(), True),
            StructField('keyword', StringType(), True)
        ])

        return tweet_schema
    except AttributeError as args:
        raise f"Error in function get_schema. Error: \n{args}"


def most_favorite_tweet(df):
    """Prints most liked tweet.

    :param df: PySpark DataFrame
    :return: None
    """
    try:
        max_favorite_count = df.agg(F.max(F.col('favorite_count'))
                                    .alias('most_favorite_count')) \
            .collect()[0]['most_favorite_count']    # Gets most likes

        # Gets the tweets having most likes
        favorite_tweet = df.select(F.col('text')) \
            .filter(
            F.col('favorite_count') == max_favorite_count)

        print(f"\nTweet with highest number of favorites: "
              f"{favorite_tweet.select('text').show(truncate=False)}")

        return None

    except Exception as args:
        raise f"Error in function most_favorite_tweet. Error: \n{args}"


def most_followed_account(df):
    """Prints the screen name of most followed account.

    :param df: PySpark DataFrame
    :return: None
    """
    try:
        # Gets the maximum follower count
        most_followers_count = df.agg(
            F.max(
                F.col('followers_count')).alias('most_follower_count')
        ) \
            .collect()[0]['most_follower_count']

        # Filters the account having maximum follower count.
        account_name = df.filter(
            F.col('followers_count') == most_followers_count)

        print(f"\nScreen Name with highest number of followers: "
              f"{account_name.select('screen_name').show(truncate=False)}")

        return None
    except Exception as args:
        raise f"Error in function most_followed_account. Error: \n{args}"


def plot_bar_chart(x, y, title, image_name, x_label="Date", y_label="Engagement rate"):
    """Plots the bar chart.

    :param x: x_axis data
    :param y: y_axis data
    :param title: title of the chart
    :param image_name: name of the chart to be saved
    :param x_label: label of the x_axis
    :param y_label: label of the y_axis
    :return: None
    """
    try:
        plt.figure(figsize=(20, 10))
        plt.bar(x, y)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.savefig(image_name)

        print(f"\nGenerated {image_name}")

        return None
    except Exception as args:
        raise f"Error in function plot_bar_chart. Error: \n{args}"


def plot_line_chart(x, y, title, image_name, x_label="Date", y_label="Engagement rate"):
    """Plots the line chart.

    :param x: x_axis data
    :param y: y_axis data
    :param title: title of the chart
    :param image_name: name of the chart to be saved
    :param x_label: label of the x_axis
    :param y_label: label of the y_axis
    :return: None
    """
    try:
        plt.figure(figsize=(20, 10))
        plt.plot(x, y)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.savefig(image_name)
        print(f"\nGenerated {image_name}")

        return None

    except Exception as args:
        raise f"Error in function plot_line_chart. Error: \n{args}"


def pre_process_text(df):
    """Clean the tweet text.

    :param df: PySpark DataFrame
    :return: PySpark DataFrame with cleaned text and created day columns.
    """
    try:
        df = df.withColumn(
            'cleaned_text',
            F.trim(
                F.regexp_replace(F.lower(F.col('text')), "([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "")
            )
        )    # Removes the special characters and urls from the tweet text

        df = df.withColumn(
            'cleaned_text',
            F.regexp_replace(F.col('cleaned_text'), " +", " ")
        )    # Removes the extra white spaces in between the texts

        print("\nCreated cleaned_text column.")

        df = df.withColumn('created_day',
                           F.to_date(F.col('created_at'))
                           )    # Converts the timestamp to date

        print("Created created_day column.")

        return df
    except Exception as args:
        raise f"Error in function pre_process_text. Error: \n{args}"


def tweet_length(df):
    """Counts the words of text

    :param df: PySpark DataFrame
    :return: PySpark DataFrame with word_count column
    """
    try:
        df = df.withColumn(
            'tweet_length',
            F.length(F.col('text'))    # Get the size of list having words
        )

        print("\nCreated tweet_length column.")

        return df
    except Exception as args:
        raise f"Error in function tweet_word_count. Error: \n{args}"
