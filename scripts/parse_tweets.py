from utils import auth_gdrive, Config, DataBase
from pydrive.drive import GoogleDrive
import os
import json
from datetime import datetime


def download_data(drive, task_id, root_folder, local_folder):
    fname_list = []
    # check if folder exists
    file_list = drive.ListFile(
        {'q': f"'{root_folder}' in parents and trashed=false"}
    ).GetList() # raw_data
    filename_list = {f["title"]: f["id"] for f in file_list}

    # downloads every file inside the folder
    if task_id in filename_list.keys():
        # create local folders if not exists
        if not os.path.exists(local_folder): os.mkdir(local_folder)
        if not os.path.exists(os.path.join(local_folder, task_id)): os.mkdir(os.path.join(local_folder, task_id))

        file_list = drive.ListFile(
        {'q': f"'{filename_list[task_id]}' in parents and trashed=false"}
    ).GetList()
        for file in file_list:
            filepath = os.path.join(local_folder, task_id, file["title"])
            file.GetContentFile(filepath)
            fname_list.append(filepath)
    else:
        raise Exception(f"task_id '{task_id}' does not exist.")

    return fname_list

def parse_datafile(drive, db, task_id, root_folder, local_folder):
    tweets = []
    users = []
    media = []
    places = []
    files = download_data(drive, task_id, root_folder, local_folder)
    for file in files:
        with open(file) as fp:
            file_content = json.load(fp)
            data = file_content["data"]
            for tweet in data:
                tweet_row = {
                    "tweet_id": tweet["tweet_id"],
                    "user_id": tweet["author_id"],
                    "task_id": task_id,
                    "text": tweet["tweet_text"],
                    "place_id": tweet["place_id"],
                    "created_at": tweet["tweet_created_at"],
                    "retweet_count": tweet["tweet_public_metrics"]["retweet_count"],
                    "reply_count": tweet["tweet_public_metrics"]["reply_count"],
                    "like_count": tweet["tweet_public_metrics"]["like_count"],
                    "quote_count": tweet["tweet_public_metrics"]["quote_count"],
                    "language": tweet["tweet_lang"],
                    "conversation_id": tweet["tweet_conversation_id"],
                    "in_reply_to_user_id": tweet["tweet_in_reply_to_user_id"],
                    "media_key": tweet["media_key"],
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                user_row = {
                    "user_id": tweet["author_id"],
                    "username": tweet["author_username"],
                    "description": tweet["author_description"],
                    "created_at": tweet["author_created_at"],
                    "location": tweet["author_location"],
                    "is_verified": tweet["author_is_verified"],
                    "is_protected": tweet["author_is_protected"],
                    "profile_image_url": tweet["author_profile_image_url"],
                    "followers_count": tweet["author_public_metrics"]["followers_count"],
                    "following_count": tweet["author_public_metrics"]["following_count"],
                    "tweet_count": tweet["author_public_metrics"]["tweet_count"],
                    "listed_count": tweet["author_public_metrics"]["listed_count"],
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                media_row = {
                    "media_key": tweet["media_key"],
                    "type": tweet["media_type"],
                    "duration_ms": tweet["media_duration_ms"],
                    "height": tweet["media_height"],
                    "width": tweet["media_width"],
                    "preview_image_url": tweet["media_preview_image_url"],
                    "alt_text": tweet["media_alt_text"],
                    "view_count":tweet["media_view_count"],
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                place_row = {
                    "place_id": tweet["place_id"],
                    "full_name": tweet["place_full_name"],
                    # "contained_within": tweet["place_contained_within"],
                    "country": tweet["place_country"],
                    "name": tweet["place_name"],
                    "type": tweet["place_type"],
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                tweets.append(tweet_row)
                users.append(user_row)
                if media_row["media_key"]:
                    media.append(media_row)
                if place_row["place_id"]:
                    places.append(place_row)

    if tweets:
        db.insert_batch(table_name="tweets", data=tweets, pkeys=["tweet_id", "task_id"])
    if users:
        db.insert_batch(table_name="users", data=users, pkeys=["user_id"])
    if media:
        db.insert_batch(table_name="media", data=media, pkeys=["media_key"])
    if places:
        db.insert_batch(table_name="places", data=places, pkeys=["place_id"])

if __name__ == "__main__":
    cfg = Config()
    gauth = auth_gdrive(client_secrets_path=cfg.credentials.get("google_drive_credentials"))
    gdrive = GoogleDrive(gauth)

    db = DataBase()
    db.connect(
        host=cfg.database.get("host"),
        database=cfg.database.get("database"),
        user=cfg.database.get("user"),
        password=cfg.database.get("password")
    )
    db.create_tables(
        tweets=cfg.database.get("tables").get("tweets"),
        users=cfg.database.get("tables").get("users"),
        media=cfg.database.get("tables").get("media"),
        places=cfg.database.get("tables").get("places")
    )
    parse_datafile(
        gdrive,
        db,
        cfg.collector.get("task_id"),
        cfg.storage.get("gdrive_folder_id"),
        cfg.storage.get("local_folder")
    )