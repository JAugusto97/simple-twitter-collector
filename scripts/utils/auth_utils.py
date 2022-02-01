import json
from yaml import safe_load
from pydrive.auth import GoogleAuth
from .collect_utils import logger

class Config:
    def __init__(self, config_path="config.yaml"):
        with open(config_path) as f:
            self.complete = safe_load(f)
            self.credentials = self.complete.get("credentials")
            self.storage = self.complete.get("storage")
            self.collector = self.complete.get("collector")

            logger.debug(
                f"""
                -----Configs-----
                Credentials:
                    Twitter Credentials: {self.credentials.get('twitter_credentials')} 
                    Google Drive Credentials: {self.credentials.get('google_drive_credentials')}
                    Has Twitter Elevated Access: {self.credentials.get('is_twitter_elevated_access')}

                Storage:
                    Dump To Google Drive: {self.storage.get('dump_to_google_drive')} 
                    Main Google Drive Folder ID: {self.storage.get('gdrive_folder_id')}
                    Local Folder: {self.storage.get('local_folder')} 

                Collector:
                    Task ID: {self.collector.get('task_id')} 
                    Query: {self.collector.get('query')}
                    Max Results: {self.collector.get('max_results')}
                    Dump Batch Size: {self.collector.get('dump_batch_size')}
                    Start Time: {self.collector.get('start_time')} 
                    End Time: {self.collector.get('end_time')}
                """ + 18*"-"
            )

def load_credentials(filename):
    with open(filename) as fp:
        credentials = json.load(fp)

    consumer_key = credentials.get("api_key")
    consumer_secret = credentials.get("api_secret")
    bearer_token = credentials.get("bearer_token")

    return consumer_key, consumer_secret, bearer_token


def auth_gdrive(client_secrets_path, cache_file="credentials/cached_google_credentials.txt"):
    gauth = GoogleAuth()             

    # Try to load cached client credentials, else launch webserver to auth
    gauth.LoadCredentialsFile(cache_file)
    gauth.DEFAULT_SETTINGS['client_config_file'] = client_secrets_path
    if gauth.credentials is None:
        logger.debug("No GDrive credentials cache found. Initializating GDrive Web Server Auth")
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        logger.debug("Refreshing Gdrive Access token.")
        gauth.Refresh()
    else:
        logger.debug("Credentials cache found.")
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(cache_file)

    return gauth

