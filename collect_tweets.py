import tweepy
import json
from datetime import datetime

CREDENTIALS_FILE = "credentials.json"

def load_credentials(filename):
    with open(filename) as fp:
        credentials = json.load(fp)

    consumer_key = credentials.get("api_key")
    consumer_secret = credentials.get("api_secret")
    bearer_token = credentials.get("bearer_token")

    return consumer_key, consumer_secret, bearer_token

def collect_tweets(client, query, max_results):
    tweets = client.search_recent_tweets(
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
    max_results=max_results,
    )

    users = {u["id"]: u for u in tweets.includes['users']}
    collected_tweets = []
    for i, tweet in enumerate(tweets.data):
        new_row = {}
        if users[tweet.author_id]:
            user = users[tweet.author_id]
            new_row = {
                "query": query,
                "timestamp": datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"),
                "tweet_text": tweet.text,
                "tweet_id": tweet.id,
                "tweet_created_at": tweet.created_at.strftime("%d-%b-%Y (%H:%M:%S.%f)"),
                "tweet_geo": tweet.geo,
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
    
    return collected_tweets

if __name__ == "__main__":
    consumer_key, consumer_secret, bearer_token = load_credentials(CREDENTIALS_FILE)
    client = tweepy.Client(bearer_token=bearer_token)

    tweets = collect_tweets(client, 'lula bolsonaro', 100)
    with open("results.json", "w") as fp:
        json.dump(tweets, fp)