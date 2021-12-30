import json
import os
import tweepy
from pydrive.drive import GoogleDrive
from utils import (
    auth_gdrive,
    collect_tweets_from_query,
    collect_tweets_from_user_id,
    get_tweet_id,
    load_credentials,
    parse_args,
    upload_file,
    TIMESTAMP
)

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

    upload_file(gdrive, data, f"{TIMESTAMP}.json", "alckimin ovos")