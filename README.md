# TweetAnalysis
This Python module reads the tweets, analysis them, and then stores them in PySpark.sql.DataFrame with respect to two
keywords. These keywords are provided by the user while running the script. The script analyses the follower count,
number of words, favorite count, and engagement rate of tweets. This module can be executed as follows,

`$python main.py keyword1 keyword2`

where the keyword1 and keyword2 represents the first keyword and second keyword respectfully. These parameters can be
provided by the user. This module outputs plots of sentiment score, engagement rate, engagment count and the words
lengths.

NOTE: Make sure add user's credentials in the config/config.ini file before running the module.