from pyspark.sql import SparkSession


def start_spark(app_name='twitter_analysis', master='local[*]'):
    """Creates the SparkSession object.
    :param app_name: app_name provided for SparkSession
    :param master: cluster manager
    :return: SparkSession object
    """
    try:
        spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
        print("SparkSession is up and running")

        return spark
    except TypeError as args:
        raise f"Error occurred while creating the SparkSession. Error: {args}"