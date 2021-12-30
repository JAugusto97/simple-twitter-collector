import json
import os
import tweepy
import argparse
from pydrive.drive import GoogleDrive
from utils import (
    auth_gdrive,
    collect_tweets_from_query,
    collect_tweets_from_user_id,
    get_tweet_id,
    load_credentials,
    upload_file,
    TIMESTAMP
)

def parse_args():
    parser = argparse.ArgumentParser(description='Collect tweets.')
    parser.add_argument(
        'news_id',
        type=str,
        help="unique news identifier"
    )
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
        default="credentials/twitter_credentials.json",
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

if __name__ == "__main__":
    args = parse_args()
    gauth = auth_gdrive()
    gdrive = GoogleDrive(gauth)  
    
    consumer_key, consumer_secret, bearer_token = load_credentials(args.credentials_file)
    client = tweepy.Client(bearer_token=bearer_token)

    if args.query:
        data = collect_tweets_from_query(client, args.query, args.max_results)
    
    elif args.username:
        tweet_id = get_tweet_id(args.user_ids_file, args.username)
        data = collect_tweets_from_user_id(client, tweet_id, args.max_results)

    upload_file(gdrive, data, f"{TIMESTAMP}.json", args.news_id)