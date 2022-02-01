import json
from yaml import safe_load
from pydrive.auth import GoogleAuth
from loguru import logger

def load_configs(config_path):
    with open(config_path) as f:
        configs = safe_load(f)
        
    credentials_cfg = configs.get("credentials")
    storage_cfg = configs.get("storage")
    collector_cfg = configs.get("collector")

    logger.debug(
        f"""
        -----Configs-----
        Credentials:
            Twitter Credentials: {credentials_cfg.get('twitter_credentials')} 
            Google Drive Credentials: {credentials_cfg.get('google_drive_credentials')}
            Has Twitter Elevated Access: {credentials_cfg.get('is_twitter_elevated_access')}

        Storage:
            Dump To Google Drive: {storage_cfg.get('dump_to_google_drive')} 
            Main Google Drive Folder ID: {storage_cfg.get('gdrive_folder_id')}
            Local Folder: {storage_cfg.get('local_folder')} 

        Collector:
            Task ID: {collector_cfg.get('task_id')} 
            Query from DB: {collector_cfg.get('query_from_db')}
            Query: {collector_cfg.get('query')}
            Max Results: {collector_cfg.get('max_results')}
            Dump Batch Size: {collector_cfg.get('dump_batch_size')}
            Start Time: {collector_cfg.get('start_time')} 
            End Time: {collector_cfg.get('end_time')}
        """ + 18*"-")

    print(
        f"""
        Collecting Tweets...
        Task ID: {collector_cfg.get('task_id')}
        Query: {collector_cfg.get('query')}
        """
    )
    return credentials_cfg, storage_cfg, collector_cfg


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

