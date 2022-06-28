import requests
import sqlalchemy
from sqlalchemy import inspect
import pandas as pd
import sys

# local imports
import docker_pipeline_week06.slackbot.postgresEnv as postgresEnv
import engine
import docker_pipeline_week06.slackbot.webhook_env as webhook_env


webhook_url = webhook_env.hookURL

# CREATE SQL ENGINE
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

# Grab one tweet from Postgres
# Query generator
query1 = 'select "compound" from sentiment FETCH FIRST ROW ONLY'
query2 = 'select "tweet"  from sentiment FETCH FIRST ROW ONLY'
#tweetData = pd.read_sql(query, enginePostgres, index_col=['Date'], columns=['Index'])
results1 = enginePostgres.execute(query1)
results2 = enginePostgres.execute(query2)
data1 = results1.fetchall()
data2 = results2.fetchall()

rating = str(data1[0]).strip("()")
tweet = str(data2[0]).strip("()")
message = f'The great Oracle of Sriracha proclaims the tweet "{tweet}", has a compound rating of {rating}.'

print(message)

# PUSH Tweets
data = {'text': message}
requests.post(url=webhook_url, json=data)