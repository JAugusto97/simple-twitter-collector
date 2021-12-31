import os
import json
from datetime import datetime
from uuid import uuid4
import tweepy
from pydrive.auth import GoogleAuth

FILE_UUID = str(uuid4())

def load_credentials(filename):
    with open(filename) as fp:
        credentials = json.load(fp)

    consumer_key = credentials.get("api_key")
    consumer_secret = credentials.get("api_secret")
    bearer_token = credentials.get("bearer_token")

    return consumer_key, consumer_secret, bearer_token

def collect_tweets_from_query(gdrive, client, query, max_results, news_id, save_batch_size):
    collected_tweets = []
    for i, tweets in enumerate(tweepy.Paginator(
        client.search_recent_tweets,
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
        expansions='author_id',
        max_results=100,
    )):
        print(f"Batch {i}")
        if (max_results and len(collected_tweets) >= max_results) or not (tweets.includes.get('users')):
            print(f"collected {len(collected_tweets)} tweets.")
            break

        users = {u["id"]: u for u in tweets.includes['users']}
        for i, tweet in enumerate(tweets.data):
            new_row = {}
            if users[tweet.author_id]:
                user = users[tweet.author_id]
                new_row = {
                    "tweet_text": tweet.text,
                    "tweet_id": tweet.id,
                    "tweet_created_at": tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "tweet_geo": tweet.geo["place_id"] if tweet.geo else None,
                    "tweet_public_metrics": tweet.public_metrics,
                    "tweet_in_reply_to_user_id": tweet.in_reply_to_user_id,
                    "tweet_conversation_id": tweet.conversation_id,
                    "tweet_lang": tweet.lang,
                    "author_id": tweet.author_id,
                    "author_name": user.name,
                    "author_username": user.username,
                    "author_created_at": user.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "author_description": user.description,
                    "author_entities": user.entities,
                    "author_location": user.location,
                    "author_is_protected": user.protected,
                    "author_is_verified": user.verified,
                    "author_profile_image_url": user.profile_image_url,
                    "author_public_metrics": user.public_metrics
                }
                collected_tweets.append(new_row)
    
                if len(collected_tweets) >= save_batch_size:
                    data = {
                        "query": query,
                        "query_id": FILE_UUID,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "data": collected_tweets,
                    }

                    upload_file(gdrive, data, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json", news_id)
                    collected_tweets = []

    data = {
        "query": query,
        "query_id": FILE_UUID,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": collected_tweets,
    }

    upload_file(gdrive, data, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json", news_id)
    
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
        if max_results and len(collected_tweets) >= max_results:
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
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": collected_tweets,
    }

    return data 


def upload_file(drive, data, filename, news_name):
    parent_id = "1xvHEH-O-VWF7gSeJ5HSo5HIOyiI1udqx" # raw_data
    local_path = os.path.join("raw_data", news_name)

    # list of folders at raw_data
    file_list = drive.ListFile(
        {'q': f"'{parent_id}' in parents and trashed=false"}
    ).GetList()
    filename_list = {f["title"]: f["id"] for f in file_list}

    # create folder if not exists
    if news_name not in filename_list.keys():
        folder_metadata = {
            'title': news_name,
            'parents': [
                {"id": f"{parent_id}", "kind": "drive#childList"}
            ],
            'mimeType': 'application/vnd.google-apps.folder'
        }

        folder = drive.CreateFile(folder_metadata)
        folder.Upload()
        folder_id = folder.get("id")
    else:
        folder_id = filename_list[news_name]

    # upload file to folder
    file_metadata = {
        'title': filename,
        'parents': [{"id": folder_id, "kind": "drive#childList"}], # eleicoes2022/raw_data/news_name
        'mimeType': 'application/json' if filename.endswith(".json") else 'application/vnd.ms-excel'
    }

    # dump file locally
    if not os.path.exists(local_path):
        os.mkdir(local_path)

    filepath = os.path.join(local_path, filename)
    with open(filepath, "w") as fp:
        json.dump(data, fp)

    # upload to google drive
    gfile = drive.CreateFile(file_metadata)
    gfile.SetContentFile(filepath)
    gfile.Upload()

def auth_gdrive(cache_file="credentials/cached_google_credentials.txt"):
    gauth = GoogleAuth()             

    # Try to load cached client credentials, else launch webserver to auth
    gauth.LoadCredentialsFile(cache_file)
    gauth.DEFAULT_SETTINGS['client_config_file'] = "credentials/client_secrets.json"
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(cache_file)

    return gauth

def get_tweet_id(filename, username):
    with open(filename) as fp:
        tweet_ids = json.load(fp)

    tweet_id = tweet_ids.get(username)
    return tweet_id