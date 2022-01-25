import tweepy
from pydrive.drive import GoogleDrive
from utils.auth_utils import auth_gdrive, load_configs, load_credentials
from utils.collect_utils import collect_tweets_default, collect_tweets_elevated
from loguru import logger
from sys import stderr
from os import getenv

loglevel = getenv("LOGLEVEL", "INFO").upper()
logger.remove()
logger.add(stderr, level=loglevel)

if __name__ == "__main__":
    credentials_cfg, storage_cfg, collector_cfg = load_configs("config.yaml")

    if not collector_cfg.get("task_id"):
        raise Exception("Missing Task ID configuration attribute")
    if not collector_cfg.get("query"):
        raise Exception("Missing search query configuration attribute")
    if not storage_cfg.get("local_folder"):
        raise Exception("Missing local folder configuration attribute")
    if not credentials_cfg.get("twitter_credentials"):
        raise Exception("Missing Twitter credentials configuration attribute")

    if storage_cfg.get("dump_to_google_drive"):
        google_credentials = credentials_cfg.get("google_drive_credentials")
        if google_credentials and storage_cfg.get("gdrive_folder_id"):
            gauth = auth_gdrive(google_credentials)
            gdrive = GoogleDrive(gauth)
        else:
            raise Exception("Missing Google Drive credentials or Folder ID configuration attributes")
    else:
        gdrive = None
    
    _, _, bearer_token = load_credentials(credentials_cfg.get("twitter_credentials"))

    client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

    if credentials_cfg.get("is_twitter_elevated_access"):
        collect_tweets_elevated(
            client=client,
            task_id=collector_cfg.get("task_id"),
            query=collector_cfg.get("query"),
            start_time=collector_cfg.get("start_time"),
            end_time=collector_cfg.get("end_time"),
            max_results=collector_cfg.get("max_results"),
            dump_batch_size=collector_cfg.get("dump_batch_size"),
            gdrive_folder_id=storage_cfg.get("gdrive_folder_id"),
            local_folder=storage_cfg.get("local_folder"),
            gdrive=gdrive
        )
    else:
        collect_tweets_default(
            client=client,
            task_id=collector_cfg.get("task_id"),
            query=collector_cfg.get("query"),
            max_results=collector_cfg.get("max_results"),
            dump_batch_size=collector_cfg.get("dump_batch_size"),
            gdrive_folder_id=storage_cfg.get("gdrive_folder_id"),
            local_folder=storage_cfg.get("local_folder"),
            gdrive=gdrive
        )
        