from utils import (
    auth_gdrive,
    upload_file,
)
from pydrive.drive import GoogleDrive
import os
import json
import argparse
from tqdm import tqdm
import mysql.connector
from datetime import datetime

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

def parse_datafile(drive, cursor, news_id):
    tweets = []
    users = []
    files = download_news_data(drive, news_id)
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

                tweets.append(tweet_row)
                users.append(user_row)

    for table_name, data in [("tweets", tweets), ("users", users)]:
        cols = ", ".join(data[0].keys())
        values = "".join([f"%({key})s," for key in data[0].keys()])[:-1]
        sql = \
            f"""INSERT INTO {table_name} ({cols})
            VALUES ({values}) ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
            """

        cursor.executemany(sql, data)

if __name__ == "__main__":
    args = parse_args()
    gauth = auth_gdrive()
    gdrive = GoogleDrive(gauth)

    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="eleicoes2022"
    )

    cursor = db.cursor()
    parse_datafile(gdrive, cursor, args.news_id)
    db.commit()

    print(cursor.rowcount, "records inserted.")


    