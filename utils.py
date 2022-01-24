import os
import json
from yaml import safe_load
from pydrive.auth import GoogleAuth


def load_configs(config_path):
    with open(config_path) as f:
        configs = safe_load(f)
        
    credentials_cfg = configs.get("credentials")
    storage_cfg = configs.get("storage")
    collector_cfg = configs.get("collector")

    print(5*"-"+"Configs:"+5*"-")
    print("Credentials:")
    print(f"Twitter Credentials: {credentials_cfg.get('twitter_credentials')}")
    print(f"Google Drive Credentials: {credentials_cfg.get('google_drive_credentials')}")

    print("\nStorage:")
    print(f"Dump To Google Drive: {storage_cfg.get('dump_to_google_drive')}")
    print(f"Main Google Drive Folder ID: {storage_cfg.get('gdrive_folder_id')}")
    print(f"Local Folder: {storage_cfg.get('local_folder')}")

    print("\nCollector:")
    print(f"Dump Batch Size: {collector_cfg.get('dump_batch_size')}")
    print(f"Start Time: {collector_cfg.get('start_time')}")
    print(f"End Time: {collector_cfg.get('end_time')}")
    print(18*"-"+"\n") 

    return credentials_cfg, storage_cfg, collector_cfg


def load_credentials(filename):
    with open(filename) as fp:
        credentials = json.load(fp)

    consumer_key = credentials.get("api_key")
    consumer_secret = credentials.get("api_secret")
    bearer_token = credentials.get("bearer_token")

    return consumer_key, consumer_secret, bearer_token

def dump_data(drive, data, filename, task_id, gdrive_folder_id, local_folder):
    if not os.path.exists(local_folder):
        os.mkdir(local_folder)

    local_path = os.path.join(local_folder, task_id)

     # dump file locally
    if not os.path.exists(local_path):
        os.mkdir(local_path)

    filepath = os.path.join(local_path, filename)
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

        # upload to google drive
        gfile = drive.CreateFile(file_metadata)
        gfile.SetContentFile(filepath)
        gfile.Upload()

def auth_gdrive(client_secrets_path, cache_file="credentials/cached_google_credentials.txt"):
    gauth = GoogleAuth()             

    # Try to load cached client credentials, else launch webserver to auth
    gauth.LoadCredentialsFile(cache_file)
    gauth.DEFAULT_SETTINGS['client_config_file'] = client_secrets_path
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