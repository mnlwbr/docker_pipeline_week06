#import libs
import tweepy
import pymongo
import sys

#import local keys
import twitter_keys


# This containerized script will collect a specific query and push it to the mongoDB container down the pipeline

## PART 1: TWITTER COLLECTION

# Connect to twitter API
#print('TOKEN: ', twitter_keys.Bearer_Token)
print('TWEET, TWEET my Lovely...\n')
client = tweepy.Client(bearer_token=twitter_keys.Bearer_Token)

# QUERY, preferably something that shows sentiment on something useful..
# - means NOT
#search_query = "alteryx -@alteryx -is:retweet -is:reply -is:quote -has:links"
search_query = "Shanghai port"

#option to extract tweets of a particular language add `lang` parameter eg lang:de

cursor = tweepy.Paginator(
    method=client.search_recent_tweets,
    query=search_query,
    tweet_fields=['author_id', 'created_at', 'public_metrics'],
    user_fields=['username'],
).flatten(limit=20)

#for tweet in cursor:
#     print(tweet.data)

## PART 2: connection to MongoDB

# creating the connection, I need the ip and the port number

client_docker = pymongo.MongoClient("my_mongodb:27017") 
#client_docker = pymongo.MongoClient(host="127.0.0.1",port=27017)
# When using docker containers to connect to a mongo instance in docker, replace the host = 'mongodb_container_name'

# creating a new mongo database in the docker container mongo
try:
    db = client_docker.tweepyStore

except Exception as e:
            print("Error: ", e)
            sys.exit(1)


#client_docker.name

## creating a new collection with the twitter data
# insert the dictionary all at once 
try:
    for tweet in cursor:
        db.tweepyStore.insert_one(tweet.data)
        print('Tweet inserted:', tweet)

except Exception as e:
            print("Error: ", e)
            sys.exit(1)

# print to whats in the DB
for document in db.tweepyStore.find():
    print(document)

