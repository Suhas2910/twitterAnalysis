import configparser
import tweepy


class TwitterAPI:
    """Created twitter api object.
    """
    def __init__(self):
        # Reads the configurations from the config file and creates the API.
        config = configparser.ConfigParser()
        config.read("/Users/suhassangolli/PycharmProjects/twitterAnalysis/config/config.ini")

        # Get the configurations from config.ini file
        self.limit = int(config['twitter']['LIMIT'])

        APP_KEY = config['twitter']['API_KEY']
        API_KEY_SECRET = config['twitter']['API_KEY_SECRET']

        ACCESS_TOKEN = config['twitter']['ACCESS_TOKEN']
        ACCESS_TOKEN_SECRET = config['twitter']['ACCESS_TOKEN_SECRET']

        # Authentication
        auth = tweepy.OAuthHandler(APP_KEY, API_KEY_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        self.api = tweepy.API(auth, wait_on_rate_limit=True)

    def read_tweets(self, query):
        """Reads the tweets from twitter api.
        :param query: the query to scrape the tweets
        :return: Cursor object containing the tweets information
        """
        try:
            tweets = tweepy.Cursor(
                self.api.search_tweets,
                q=query,
                count=100,
                tweet_mode='extended'
            ).items(self.limit)

            return tweets
        except TypeError as args:
            raise f"Error while collecting tweets. ERROR:{args}"