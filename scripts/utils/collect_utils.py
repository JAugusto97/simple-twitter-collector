from uuid import uuid4
from datetime import datetime
import os
from loguru import logger
import json
import tweepy
from sys import stderr

loglevel = os.getenv("LOGLEVEL", "INFO").upper()
logger.remove()
logger.add(stderr, level=loglevel)

def get_tweet_id(filename, username):
    with open(filename) as fp:
        tweet_ids = json.load(fp)

    tweet_id = tweet_ids.get(username)
    return tweet_id

def dump_data(drive, data, filename, task_id, gdrive_folder_id, local_folder):
    if not os.path.exists(local_folder):
        os.mkdir(local_folder)

    local_path = os.path.join(local_folder, task_id)

     # dump file locally
    if not os.path.exists(local_path):
        os.mkdir(local_path)

    filepath = os.path.join(local_path, filename)
    logger.debug(f"LOCAL: Dumping {filename} to {local_path}")
    with open(filepath, "w") as fp:
        json.dump(data, fp)

    if drive:
        parent_id = gdrive_folder_id
        # list of folders at raw_data
        file_list = drive.ListFile(
            {'q': f"'{parent_id}' in parents and trashed=false"}
        ).GetList()
        filename_list = {f["title"]: f["id"] for f in file_list}

        # create folder if not exists
        if task_id not in filename_list.keys():
            folder_metadata = {
                'title': task_id,
                'parents': [
                    {"id": f"{parent_id}", "kind": "drive#childList"}
                ],
                'mimeType': 'application/vnd.google-apps.folder'
            }

            folder = drive.CreateFile(folder_metadata)
            folder.Upload()
            folder_id = folder.get("id")
        else:
            folder_id = filename_list[task_id]

        # upload file to folder
        file_metadata = {
            'title': filename,
            'parents': [{"id": folder_id, "kind": "drive#childList"}],
            'mimeType': 'application/json' if filename.endswith(".json") else 'application/vnd.ms-excel'
        }

        logger.debug(f"GDRIVE: Dumping {filename} to {task_id}")
        # upload to google drive
        gfile = drive.CreateFile(file_metadata)
        gfile.SetContentFile(filepath)
        gfile.Upload()

def collect_tweets_elevated(
    gdrive,
    client,
    query,
    start_time,
    end_time,
    max_results,
    task_id,
    dump_batch_size,
    gdrive_folder_id,
    local_folder
):
    collected_tweets = []
    total_collected = 0
    for i, tweets in enumerate(tweepy.Paginator(
        client.search_all_tweets,
        start_time=start_time,
        end_time=end_time,
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
        max_results=100
    )):
        logger.info(f"Retrieved page {i+1} -- Collected {total_collected} tweets so far.")
        if (max_results and len(collected_tweets) >= max_results) or not (tweets.includes.get('users')):
            break

        # get expansions
        users = {u["id"]: u for u in tweets.includes['users']}
        media = {media["media_key"]: media for media in tweets.includes.get("media")} if tweets.includes.get("media") else None
        places = {place["id"]: place for place in tweets.includes.get("places")} if tweets.includes.get("places") else None


        # get tweet info inside each object
        for i, tweet in enumerate(tweets.data):
            new_row = {}
        
            user = users[tweet.author_id]
            place = places[tweet.geo["place_id"]] if places and tweet.get("geo") else None

            # get media
            attachments = tweet['attachments']
            media_keys = attachments.get('media_keys') if attachments else None
            tweet_media = media.get(media_keys[0] if media_keys else media_keys) if media else None

            new_row = {
                "tweet_text": tweet.text,
                "tweet_id": tweet.id,
                "tweet_created_at": tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "tweet_public_metrics": tweet.public_metrics,
                "tweet_in_reply_to_user_id": tweet.in_reply_to_user_id,
                "tweet_conversation_id": tweet.conversation_id,
                "tweet_lang": tweet.lang,
                "tweet_context_annotations": tweet.context_annotations,

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
                "media_type": tweet_media.get("type") if tweet_media else None,
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
            total_collected += 1

            if dump_batch_size and len(collected_tweets) >= dump_batch_size:
                data = {
                    "query": query,
                    "query_id": str(uuid4()),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "data": collected_tweets,
                }
                dump_data(
                    drive=gdrive,
                    data=data,
                    filename=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json",
                    task_id=task_id,
                    gdrive_folder_id=gdrive_folder_id,
                    local_folder=local_folder
                )
                collected_tweets = []

    data = {
        "query": query,
        "query_id": str(uuid4()),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": collected_tweets,
    }

    dump_data(
        drive=gdrive,
        data=data,
        filename=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json",
        task_id=task_id,
        gdrive_folder_id=gdrive_folder_id,
        local_folder=local_folder
    )

    print(f"Execution ended. Collected {total_collected} tweets.")

def collect_tweets_default(
    gdrive,
    client,
    query,
    max_results,
    task_id,
    dump_batch_size,
    gdrive_folder_id,
    local_folder
):
    collected_tweets = []
    total_collected = 0
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
        expansions = [
            "author_id",
            "referenced_tweets.id",
            "referenced_tweets.id.author_id",
            "attachments.media_keys",
        ],
        max_results=100
    )):
        logger.info(f"Retrieved page {i+1} -- Collected {total_collected} tweets so far.")
        if (max_results and len(collected_tweets) >= max_results) or not (tweets.includes.get('users')):
            break

        # get expansions
        users = {u["id"]: u for u in tweets.includes['users']}
        media = {media["media_key"]: media for media in tweets.includes.get("media")} if tweets.includes.get("media") else None


        # get tweet info inside each object
        for i, tweet in enumerate(tweets.data):
            new_row = {}
        
            user = users[tweet.author_id]

            # get media
            attachments = tweet['attachments']
            media_keys = attachments.get('media_keys') if attachments else None
            tweet_media = media.get(media_keys[0] if media_keys else media_keys) if media else None

            new_row = {
                "tweet_text": tweet.text,
                "tweet_id": tweet.id,
                "tweet_created_at": tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "tweet_public_metrics": tweet.public_metrics,
                "tweet_in_reply_to_user_id": tweet.in_reply_to_user_id,
                "tweet_conversation_id": tweet.conversation_id,
                "tweet_lang": tweet.lang,
                "tweet_context_annotations": tweet.context_annotations,

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
                )
            }

            collected_tweets.append(new_row)
            total_collected += 1

            if dump_batch_size and len(collected_tweets) >= dump_batch_size:
                data = {
                    "query": query,
                    "query_id": str(uuid4()),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "data": collected_tweets,
                }
                dump_data(
                    drive=gdrive,
                    data=data,
                    filename=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json",
                    task_id=task_id,
                    gdrive_folder_id=gdrive_folder_id,
                    local_folder=local_folder
                )
                collected_tweets = []

    data = {
        "query": query,
        "query_id": str(uuid4()),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": collected_tweets,
    }

    dump_data(
        drive=gdrive,
        data=data,
        filename=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.json",
        task_id=task_id,
        gdrive_folder_id=gdrive_folder_id,
        local_folder=local_folder
    )

    print(f"Execution ended. Collected {total_collected} tweets.")