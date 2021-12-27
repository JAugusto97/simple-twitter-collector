import os
import argparse
import json
from datetime import datetime
from uuid import uuid4
import tweepy


FILE_UUID = str(uuid4())
TIMESTAMP = datetime.now().strftime("%d-%b-%Y_%H:%M:%S")

def load_credentials(filename):
    with open(filename) as fp:
        credentials = json.load(fp)

    consumer_key = credentials.get("api_key")
    consumer_secret = credentials.get("api_secret")
    bearer_token = credentials.get("bearer_token")

    return consumer_key, consumer_secret, bearer_token

def collect_tweets_from_query(client, query, max_results):
    collected_tweets = []
    for i, tweets in enumerate(tweepy.Paginator(
        client.search_recent_tweets,
        query=query,
        tweet_fields = [
            'context_annotations',
            'created_at',
            'geo',
            'lang',
            'public_metrics',
            'referenced_tweets',
            'in_reply_to_user_id',
        ],
        user_fields = [
            'profile_image_url',
            'name',
            'username',
            'created_at',
            'description',
            'entities',
            'location',
            'protected',
            'public_metrics',
            'verified',
        ],
        expansions='author_id',
        max_results=100,
    )):
        print(f"Batch {i}")
        if len(collected_tweets) >= max_results:
            break

        users = {u["id"]: u for u in tweets.includes['users']}
        for i, tweet in enumerate(tweets.data):
            new_row = {}
            if users[tweet.author_id]:
                user = users[tweet.author_id]
                new_row = {
                    "tweet_text": tweet.text,
                    "tweet_id": tweet.id,
                    "tweet_created_at": tweet.created_at.strftime("%d-%b-%Y_%H:%M:%S"),
                    "tweet_geo": tweet.geo["place_id"] if tweet.geo else None,
                    "tweet_public_metrics": tweet.public_metrics,
                    "tweet_in_reply_to_user_id": tweet.in_reply_to_user_id,
                    "tweet_conversation_id": tweet.conversation_id,
                    "tweet_lang": tweet.lang,
                    "author_id": tweet.author_id,
                    "author_name": user.name,
                    "author_username": user.username,
                    "author_created_at": user.created_at.strftime("%d-%b-%Y_%H:%M:%S"),
                    "author_description": user.description,
                    "author_entities": user.entities,
                    "author_location": user.location,
                    "author_is_protected": user.protected,
                    "author_is_verified": user.verified,
                    "author_profile_image_url": user.profile_image_url,
                    "author_public_metrics": user.public_metrics
                }
                collected_tweets.append(new_row)
    
    data = {
        "query": query,
        "query_id": FILE_UUID,
        "timestamp": TIMESTAMP,
        "data": collected_tweets,
    }

    return data 

def collect_tweets_from_user_id(client, user_id, max_results):
    collected_tweets = []
    for i, tweets in enumerate(tweepy.Paginator(
        client.get_users_tweets,
        id=user_id,
        tweet_fields = [
            'context_annotations',
            'created_at',
            'geo',
            'lang',
            'public_metrics',
            'referenced_tweets',
            'in_reply_to_user_id',
        ],
        user_fields = [
            'profile_image_url',
            'name',
            'username',
            'created_at',
            'description',
            'entities',
            'location',
            'protected',
            'public_metrics',
            'verified',
        ],
        expansions='author_id',
        max_results=100,
    )):
        print(f"Batch {i}")
        if len(collected_tweets) >= max_results:
            break

        users = {u["id"]: u for u in tweets.includes['users']}
        for i, tweet in enumerate(tweets.data):
            new_row = {}
            if users[tweet.author_id]:
                user = users[tweet.author_id]
                new_row = {
                    "tweet_text": tweet.text,
                    "tweet_id": tweet.id,
                    "tweet_created_at": tweet.created_at.strftime("%d-%b-%Y (%H:%M:%S.%f)"),
                    "tweet_geo": tweet.geo["place_id"] if tweet.geo else None,
                    "tweet_public_metrics": tweet.public_metrics,
                    "tweet_in_reply_to_user_id": tweet.in_reply_to_user_id,
                    "tweet_conversation_id": tweet.conversation_id,
                    "tweet_lang": tweet.lang,
                    "author_id": tweet.author_id,
                    "author_name": user.name,
                    "author_username": user.username,
                    "author_created_at": user.created_at.strftime("%d-%b-%Y (%H:%M:%S.%f)"),
                    "author_description": user.description,
                    "author_entities": user.entities,
                    "author_location": user.location,
                    "author_is_protected": user.protected,
                    "author_is_verified": user.verified,
                    "author_profile_image_url": user.profile_image_url,
                    "author_public_metrics": user.public_metrics
                }
                collected_tweets.append(new_row)
    
    data = {
        "query": user_id,
        "query_id": FILE_UUID,
        "timestamp": TIMESTAMP,
        "data": collected_tweets,
    }

    return data 


def dump_file(tweets, filename):
    if not os.path.exists("raw_data"):
        os.mkdir("raw_data")

    filepath = os.path.join("raw_data", filename)
    with open(filepath, "w") as fp:
        json.dump(tweets, fp)

def parse_args():
    parser = argparse.ArgumentParser(description='Collect tweets.')
    parser.add_argument(
        '--query',
        default=None,
        type=str,
        help="string to query twitter."
        "reference: https://developer.twitter.com/en/docs/twitter-api/v1/rules-and-filtering/search-operators"
    )
    parser.add_argument(
        '--username',
        default=None, 
        type=str,
        help="twitter username (@) to collect tweets from"
    )
    parser.add_argument(
        '--max_results', 
        type=int,
        default=None,
        help='defines maximum amount of tweets to collect. None collects everything.'
    )
    parser.add_argument(
        '--credentials_file', 
        type=str,
        default="credentials.json",
        help='json file path containing twitter api credentials'
    )
    parser.add_argument(
        '--user_ids_file', 
        type=str,
        default="user_ids.json",
        help='json file path containing usernames to user_id mappings'
    )

    args = parser.parse_args()
    query = args.query
    username = args.username

    if not query and not username:
        raise Exception("Missing Query or Username")

    return args

def get_tweet_id(filename, username):
    with open(filename) as fp:
        tweet_ids = json.load(fp)

    tweet_id = tweet_ids.get(username)
    return tweet_id