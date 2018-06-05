
# coding: utf-8

from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import client
import pandas

db = GraphDatabase("http://localhost:7474", username="neo4j", password="passy")
coin = db.labels.create("cryptocurrencies")
tweet = db.labels.create("tweet")
user = db.labels.create("user")

# add single datapoints (for streaming/batch data)

def create_new_datapoint_crypto(crypto_name, close_price, time):
    c1 = db.nodes.create(name=crypto_name, timestamp = time, price = close_price)
    coin.add(c1)
    
def create_new_datapoint_tweet(tweet_object, time):
    # get text from the tweet
    # get the hashtag list from the tweet
    t1 = db.nodes.create(name=tweet_text, hashtags = [], timestamp = time)
    # get username from the tweet_object
    # create_new_datapoint
    tweet.add(t1)
    
    # also add a relationship for the coin :)

def create_tweetcount(crypto_name, tweetcount, close_price, time):
    tc1 = db.nodes.create(name=crypto_name, count=tweetcount, timestamp = time)
    c1 = db.nodes.create(name=crypto_name, timestamp = time, price = close_price)
    coin.add(c1)
    tweet.add(tc1)
    tc1.relationships.create("count of", c1)
    
def create_new_datapoint_user(username):
    # Check whether user already exists
    u1 = db.nodes.create(name=username)
    user.add(u1)
    u1.relationships.create("tweeted", t1)
