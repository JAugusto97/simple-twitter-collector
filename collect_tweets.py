import json
import os
import tweepy
import argparse
from datetime import datetime
from pydrive.drive import GoogleDrive
from uuid import uuid4
from utils import (
    auth_gdrive,
    get_tweet_id,
    load_credentials,
    upload_file,
)

def collect_tweets_from_query(gdrive, client, query, max_results, news_id, save_batch_size):
    collected_tweets = []
    for i, tweets in enumerate(tweepy.Paginator(
        client.search_all_tweets,
        start_time="2022-01-01T00:00:00Z",
        # end_time="2023-01-01T00:00:00Z",
        query=query,
        tweet_fields = [
            'context_annotations',
            'created_at',
            'geo',
            'lang',
            'conversation_id',
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
        media_fields = [
            "media_key",
            "type",
            "duration_ms",
            "public_metrics",
            "width",
            "height",
            "preview_image_url",
            "alt_text"
        ],
        place_fields = [
            "full_name",
            "id",
            "contained_within",
            "country",
            "country_code",
            "geo",
            "name",
            "place_type"
        ],
        expansions = [
            "author_id",
            "referenced_tweets.id",
            "referenced_tweets.id.author_id",
            "attachments.media_keys",
            "geo.place_id"
        ],
        max_results=100,
    )):
        print(f"Batch {i}")
        if (max_results and len(collected_tweets) >= max_results) or not (tweets.includes.get('users')):
            print(f"collected {len(collected_tweets)} tweets.")
            break

        # get expansions
        users = {u["id"]: u for u in tweets.includes['users']}
        referenced_tweets = {referenced_tweet.id: referenced_tweet for referenced_tweet in tweets.includes.get("tweets")}
        media = {media["media_key"]: media for media in tweets.includes.get("media")} if tweets.includes.get("media") else None
        places = {place["id"]: place for place in tweets.includes.get("places")} if tweets.includes.get("places") else None


        # get tweet info inside each object
        for i, tweet in enumerate(tweets.data):
            new_row = {}
        
            user = users[tweet.author_id]
            place = places[tweet.geo["place_id"]] if places and tweet.get("geo") else None

            # get midia
            attachments = tweet['attachments']
            media_keys = attachments.get('media_keys') if attachments else None
            tweet_media = media.get(media_keys[0] if media_keys else media_keys) if media else None

            # get referenced tweet text if its a retweet (otherwise text is truncated)
            if tweet.referenced_tweets and referenced_tweets.get(tweet.referenced_tweets[0].id):
                tweet_text = referenced_tweets.get(tweet.referenced_tweets[0].id).text 
            else:
                tweet_text = tweet.text

            new_row = {
                "tweet_text": tweet_text,
                "tweet_id": tweet.id,
                "tweet_created_at": tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "tweet_public_metrics": tweet.public_metrics,
                "tweet_in_reply_to_user_id": tweet.in_reply_to_user_id,
                "tweet_conversation_id": tweet.conversation_id,
                "tweet_lang": tweet.lang,

                "author_id": user.id,
                "author_name": user.name,
                "author_username": user.username,
                "author_created_at": user.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "author_description": user.description,
                "author_entities": user.entities,
                "author_location": user.location,
                "author_is_protected": user.protected,
                "author_is_verified": user.verified,
                "author_profile_image_url": user.profile_image_url,
                "author_public_metrics": user.public_metrics,

                "media_key": tweet_media.get("media_key") if tweet_media else None,
                "media_type": tweet_media.get("media_type") if tweet_media else None,
                "media_duration_ms": tweet_media.get("duration_ms") if tweet_media else None,
                "media_height": tweet_media.get("height") if tweet_media else None,
                "media_width": tweet_media.get("width") if tweet_media else None,
                "media_preview_image_url": tweet_media.get("preview_image_url") if tweet_media else None,
                "media_alt_text": tweet_media.get("alt_text") if tweet_media else None,
                "media_view_count": (
                    tweet_media.get("public_metrics").get("view_count")
                        if tweet_media and 
                            tweet_media.get("public_metrics") and
                            tweet_media["public_metrics"].get("view_count")
                        else None
                ),
                
                "place_id": place.get("id") if place else None,
                "place_full_name": place.get("full_name") if place else None,
                "place_contained_within": place.get("contained_within") if place else None,
                "place_country": place.get("country") if place else None,
                "place_geo": place.get("geo") if place else None,
                "place_name": place.get("name") if place else None,
                "place_type": place.get("place_type") if place else None,
            }

            collected_tweets.append(new_row)

            if len(collected_tweets) >= save_batch_size:
                data = {
                    "query": query,
                    "query_id": str(uuid4()),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "data": collected_tweets,
                }

                upload_file(gdrive, data, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json", news_id)
                collected_tweets = []

    data = {
        "query": query,
        "query_id": str(uuid4()),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": collected_tweets,
    }

    upload_file(gdrive, data, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json", news_id)

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
        '--save_batch_size', 
        type=int,
        default=5000,
        help='save when batch_size tweets are collected'
    )


    return args

if __name__ == "__main__":
    args = parse_args()
    gauth = auth_gdrive()
    gdrive = GoogleDrive(gauth)  
    
    consumer_key, consumer_secret, bearer_token = load_credentials(args.credentials_file)
    client = tweepy.Client(bearer_token=bearer_token)

    if args.query:
        data = collect_tweets_from_query(gdrive, client, args.query, args.max_results, args.news_id, args.save_batch_size)
    else:
        raise Exception("Missing Query")
