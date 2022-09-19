from pyspark.sql import functions as F, Window
from tasks.tasks import plot_line_chart, plot_bar_chart


class EngagementRateAnalysis:
    """Analysis the engagement rate of tweets.

    EngagementRateAnalysis class generates the periodic plots and daily plots with respect to engagement rate.
    :param df: PySpark Dataframe
    :param keyword1: first keyword
    :param keyword2: second keyword
    :return: None
    """

    def __init__(self, df, keyword1, keyword2):
        # Two dataframes are created by filtering the original dataframe with respect to keywords.
        self.df = df
        # Calculate engagement rate
        self.calculate_eng_rate()

        self.keyword1 = keyword1
        self.keyword2 = keyword2

        self.df1 = self.df.filter(F.col('keyword') == self.keyword1)  # Filter data having keyword1
        self.df2 = self.df.filter(F.col('keyword') == self.keyword2)  # Filter data having keyword2

    def calculate_eng_rate(self):
        """Calculate the engagement rate.

        calculate_eng_rate function calculates the engagement count by adding
        retweet count, favorite count, and hashtag count of a tweet. This function
        calculates the error rate as ERROR RATE = engagement_count/followers count.
        :param df: PySpark DataFrame
        :return: PySpark DataFrame containing engagement_count and engagement_rate columns
        """
        try:
            self.df = self.df.withColumn('engagement_count',
                                         F.col('retweet_count')
                                         + F.col('favorite_count')
                                         + F.col('hashtag_count')
                                         )
            print("Created engagement_count column.")
            self.df = self.df.withColumn('engagement_rate',
                                         F.round(
                                             F.col('engagement_count')
                                             / F.col('followers_count')
                                         )
                                         )

            print("\nCreated engagement_rate column.")

        except Exception as args:
            raise f"Error in function calculate_eng_rate. ERROR: \n{args}"

    def average_engagement_rate_daily(self):
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
            )  # Creates a window by partitioning the data by day and sorting the data by tweet created date

            self.df = self.df.withColumn(
                'daily_avg_engg_rate',
                F.avg(
                    F.col('engagement_rate')
                ).over(windowSpec)
            )  # Calculates the average engagement rate with respect to window

            print("\nCreated daily_avg_engg_rate column.")

        except Exception as args:
            raise f"Error in function average_engagement_rate_daily. ERROR: \n{args}"

    def get_dataframe(self):
        return self.df

    def create_eng_period_chart(self):
        """Generates engagement plots.

        create_eng_period_chart function generates the engagement rate charts for both keywords.
        """
        try:
            df1 = self.df1.orderBy(F.col('created_at')).toPandas()  # PySpark dataframe converted to pandas dataframe
            df2 = self.df2.orderBy(F.col('created_at')).toPandas()  # PySpark dataframe converted to pandas dataframe

            title1 = f"Engagement rate of {self.keyword1}."
            image_name1 = f"engagement_rate_{self.keyword1}_plot.png"
            plot_line_chart(
                x=df1.created_at,
                y=df1.engagement_rate,
                title=title1,
                image_name=image_name1
            )

            title2 = f"Engagement rate of {self.keyword2}."
            image_name2 = f"engagement_rate_{self.keyword2}_plot.png"
            plot_line_chart(
                x=df2.created_at,
                y=df2.engagement_rate,
                title=title2,
                image_name=image_name2
            )
            return None
        except Exception as args:
            raise f"Error in function create_eng_period_chart. ERROR: \n{args}"

    def create_daily_chart(self, keyword='both'):
        """Generates bar plots with respect to daily engagement count.

        create_daily_chart function generates the bar charts with respect to keyword1, keyword2, or both.
        :param keyword: the input keyword for which the bar chart is needed
        :return: None
        """
        try:
            if keyword == self.keyword1:
                title = f"Per day twitter engagement count of {self.keyword1}."
                image_name = f"per_day_{self.keyword1}_eng_count.png"

                # Groups the data by day, sums up the engagement count, and sorts the data by day.
                # Converts the PySpark DataFrame to pandas DataFrame
                df1 = self.df1.groupBy(
                    F.col('created_day')
                ).agg(
                    F.sum(F.col('engagement_count')).alias('engagement_count_per_day')
                ).orderBy(
                    F.col('created_day')
                ).toPandas()

                # Generate the bar chart for keyword1
                plot_bar_chart(
                    x=df1.created_day,
                    y=df1.engagement_count_per_day,
                    title=title,
                    image_name=image_name,
                    y_label="Engagement Count"
                )
            elif keyword == self.keyword2:
                title = f"Per day twitter engagement count of {self.keyword2}."
                image_name = f"per_day_{self.keyword2}_eng_count.png"

                # Groups the data by day, sums up the engagement count, and sorts the data by day.
                # Converts the PySpark DataFrame to pandas DataFrame
                df2 = self.df2.groupBy(
                    F.col('created_day')
                ).agg(
                    F.sum(F.col('engagement_count')).alias('engagement_count_per_day')
                ).orderBy(
                    F.col('created_day')
                ).toPandas()

                # Generate the bar chart for keyword2
                plot_bar_chart(
                    x=df2.created_day,
                    y=df2.engagement_count_per_day,
                    title=title,
                    image_name=image_name
                )
            elif keyword == 'both':
                title = f"Per day twitter engagement count of both keywords."
                image_name = f"per_day_all_eng_count.png"

                # Groups the data by day, sums up the engagement count, and sorts the data by day.
                # Converts the PySpark DataFrame to pandas DataFrame
                df = self.df.groupBy(
                    F.col('created_day')
                ).agg(
                    F.sum(F.col('engagement_count')).alias('engagement_count_per_day')
                ).orderBy(
                    F.col('created_day')
                ).toPandas()

                # Generate the bar chart for both keywords
                plot_bar_chart(
                    x=df.created_day,
                    y=df.engagement_count_per_day,
                    title=title,
                    image_name=image_name
                )
            else:
                print("\nWrong parameter. Check the parameter.")
            return None
        except Exception as args:
            raise f"Error in function create_daily_chart. ERROR:\n{args}"
