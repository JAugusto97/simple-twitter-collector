import os
import json
from datetime import datetime
import tweepy
from pydrive.auth import GoogleAuth

def load_credentials(filename):
    with open(filename) as fp:
        credentials = json.load(fp)

    consumer_key = credentials.get("api_key")
    consumer_secret = credentials.get("api_secret")
    bearer_token = credentials.get("bearer_token")

    return consumer_key, consumer_secret, bearer_token

def upload_file(drive, data, filename, news_name):
    parent_id = "1xvHEH-O-VWF7gSeJ5HSo5HIOyiI1udqx" # raw_data
    if not os.path.exists("raw_data"):
        os.mkdir("raw_data")

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