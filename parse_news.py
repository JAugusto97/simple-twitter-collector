from utils import (
    auth_gdrive,
    upload_file,
    TIMESTAMP
)
from pydrive.drive import GoogleDrive
import os
import json
import argparse
import pandas as pd
from tqdm import tqdm

def parse_args():
    parser = argparse.ArgumentParser(description='Collect tweets.')
    parser.add_argument(
        'news_id',
        type=str,
        help="unique news identifier"
    )

    args = parser.parse_args()
    return args

def download_news_data(drive, news_id):
    fname_list = []
    # check if news folder exists
    file_list = drive.ListFile(
        {'q': "'1xvHEH-O-VWF7gSeJ5HSo5HIOyiI1udqx' in parents and trashed=false"}
    ).GetList() # raw_data
    filename_list = {f["title"]: f["id"] for f in file_list}

    # downloads every file inside the folder
    if news_id in filename_list.keys():
        # create local folders if not exists
        if not os.path.exists("raw_data"): os.mkdir("raw_data")
        if not os.path.exists(os.path.join("raw_data", news_id)): os.mkdir(os.path.join("raw_data", news_id))

        news_file_list = drive.ListFile(
        {'q': f"'{filename_list[news_id]}' in parents and trashed=false"}
    ).GetList()
        for file in news_file_list:
            filepath = os.path.join("raw_data", news_id, file["title"])
            file.GetContentFile(filepath)
            fname_list.append(filepath)
    else:
        raise Exception(f"news_id '{news_id}' does not exist.")

    return fname_list

def parse_datafile(drive, news_id):
    files = download_news_data(drive, news_id)
    tweets_df = pd.DataFrame(
        columns=[
            "tweet_id",
            "user_id",
            "news_id",
            "text",
            "created_at",
            "geo",
            "retweet_count",
            "reply_count",
            "like_count",
            "quote_count",
            "language",
            "conversation_id",
            "in_reply_to_user_id",
            "media_url",
            "updated_at"
        ]
    )
    users_df = pd.DataFrame(
        columns=[
            "user_id",
            "username",
            "description",
            "created_at",
            "location",
            "is_verified",
            "is_protected",
            "profile_image_url",
            "followers_count",
            "following_count",
            "tweet_count",
            "listed_count",
            "entities",
            "updated_at"
        ]
    )
    for file in files:
        with open(file) as fp:
            file_content = json.load(fp)
            data = file_content["data"]
            for tweet in tqdm(data):
                tweet_row = {
                    "tweet_id": tweet["tweet_id"],
                    "user_id": tweet["author_id"],
                    "news_id": news_id,
                    "text": tweet["tweet_text"],
                    "created_at": tweet["tweet_created_at"],
                    "geo": tweet["tweet_geo"],
                    "retweet_count": tweet["tweet_public_metrics"]["retweet_count"],
                    "reply_count": tweet["tweet_public_metrics"]["reply_count"],
                    "like_count": tweet["tweet_public_metrics"]["like_count"],
                    "quote_count": tweet["tweet_public_metrics"]["quote_count"],
                    "language": tweet["tweet_lang"],
                    "conversation_id": tweet["tweet_conversation_id"],
                    "in_reply_to_user_id": tweet["tweet_in_reply_to_user_id"],
                    "media_url": None,
                    "updated_at": TIMESTAMP
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
                    "updated_at": TIMESTAMP
                }

                tweets_df = tweets_df.append(tweet_row, ignore_index=True)
                users_df = users_df.append(user_row, ignore_index=True)

    tweets_df = tweets_df.drop_duplicates(["tweet_id"]).reset_index(drop=True)
    users_df = users_df.drop_duplicates(["user_id"]).reset_index(drop=True)

    return tweets_df, users_df

if __name__ == "__main__":
    args = parse_args()
    gauth = auth_gdrive()
    gdrive = GoogleDrive(gauth)

    tweets_df, users_df = parse_datafile(gdrive, args.news_id)
    upload_file(gdrive, tweets_df, "tweets.csv", args.news_id)
    upload_file(gdrive, tweets_df, "users.csv", args.news_id)


    