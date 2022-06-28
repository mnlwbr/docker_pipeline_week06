# imports
import imp
import pymongo
import sqlalchemy
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
import sys
from sqlalchemy import inspect
import time
import psycopg2

# local import of postgres credentials
import postgresEnv
import engine

"""
This script will query the mongoDB container 'my_mongodb' for the raw twitter data.

Then it will run some ETL (Extraction Transformation and loading of data.
- 

It will then transfer the data to a Postgres instance.
"""

# Getting the data from MongoDB

#   Establish connection
client_docker = pymongo.MongoClient("my_mongodb:27017")

#   Connect to db
try:
    db = client_docker.tweepyStore
    time.sleep(10)

except Exception as e:
            print("Error: ", e)
            sys.exit(1)

#   Define collection
collection = db.tweepyStore

#   Get data
#for document in db.tweepyStore.find():
#    print(document)

allTweets = pd.DataFrame(list(collection.find()))
print('\n')
print('\n')
print('\n')
print(allTweets.head(5))
print('\n')
print('\n')
print('\n')


# ETL process

#   calling the VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

#   Putting the data into a pd dataframe

dfTweets = pd.DataFrame({'tweets' : allTweets.text})

print('dfTweets BEFORE clean:\n', dfTweets.head(5)) 

#   Running some cleaning (regex script)
mentions_regex= '@[A-Za-z0-9]+'
url_regex='https?:\/\/\S+' #this will not catch all possible URLs
hashtag_regex= '#'
rt_regex= 'RT\s'

def clean_tweets(tweet):
    tweet = re.sub(mentions_regex, '', tweet)  #removes @mentions
    tweet = re.sub(hashtag_regex, '', tweet) #removes hashtag symbol
    tweet = re.sub(rt_regex, '', tweet) #removes RT to announce retweet
    tweet = re.sub(url_regex, '', tweet) #removes most URLs
    
    return tweet


dfTweets.tweets = dfTweets.tweets.apply(clean_tweets)

print('dfTweets clean:\n', dfTweets.head(5))

#   Run the vader sentiment analyzer
#   Output is a dataframe with tweettext and sentiment
sentimentScore = dfTweets['tweets'].apply(analyzer.polarity_scores).apply(pd.Series)
sentimentScore['tweet'] = dfTweets['tweets']

print(sentimentScore.head(10))




# Transfering the data to the postgres container

# Creating the SQL-Engine
## SQL-Engine Connection String


print('\n')
print('\n')
print('\n')
print('\n')
print('\n')
print('\n')
conn_string = f'postgresql://{engine.USER}:{engine.PASSWORD}@{engine.HOST}:{engine.PORT}/{engine.DATABASE}'
print(conn_string)
print('\n')
print('\n')
print('\n')
print('\n')
print('\n')
print('\n')
## Actual engine
### turn on echo=True for a more verbose output to see the raw SQL being executed for you under the hood! 
enginePostgres = sqlalchemy.create_engine(conn_string,echo=False, pool_pre_ping = True)

# Inspector object
inspector = inspect(enginePostgres)

## Engine connection function
connection = enginePostgres.connect() 

# create the necessary table
# not need
""" enginePostgres.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
    text VARCHAR(500),
    sentiment NUMERIC
);
''') """

# put stuff into the table sentiment, if it does not exist create it, if it does exist, replace it
try:
    print('Saving sentiment to Postgres-DB\n')
    sentimentScore.to_sql('sentiment', enginePostgres, if_exists='replace')

except Exception as e:
    print("Error: ", e)
    sys.exit(1)

