import tweepy
from pydrive.drive import GoogleDrive
from utils import auth_gdrive, load_credentials, collect_tweets_default, collect_tweets_elevated, Config

if __name__ == "__main__":
    cfg = Config()

    if not cfg.collector.get("task_id"):
        raise Exception("Missing Task ID configuration attribute")
    if not cfg.collector.get("query"):
        raise Exception("Missing search query configuration attribute")
    if not cfg.storage.get("local_folder"):
        raise Exception("Missing local folder configuration attribute")
    if not cfg.credentials.get("twitter_credentials"):
        raise Exception("Missing Twitter credentials configuration attribute")

    if cfg.storage.get("dump_to_google_drive"):
        google_credentials = cfg.credentials.get("google_drive_credentials")
        if google_credentials and cfg.storage.get("gdrive_folder_id"):
            gauth = auth_gdrive(google_credentials)
            gdrive = GoogleDrive(gauth)
        else:
            raise Exception("Missing Google Drive credentials or Folder ID configuration attributes")
    else:
        gdrive = None
    
    _, _, bearer_token = load_credentials(cfg.credentials.get("twitter_credentials"))

    client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

    if cfg.credentials.get("is_twitter_elevated_access"):
        collect_tweets_elevated(
            client=client,
            task_id=cfg.collector.get("task_id"),
            query=cfg.collector.get("query"),
            start_time=cfg.collector.get("start_time"),
            end_time=cfg.collector.get("end_time"),
            max_results=cfg.collector.get("max_results"),
            dump_batch_size=cfg.collector.get("dump_batch_size"),
            gdrive_folder_id=cfg.storage.get("gdrive_folder_id"),
            local_folder=cfg.storage.get("local_folder"),
            gdrive=gdrive,
            recent=cfg.collector.get("recent")
        )
    else:
        collect_tweets_default(
            client=client,
            task_id=cfg.collector.get("task_id"),
            query=cfg.collector.get("query"),
            max_results=cfg.collector.get("max_results"),
            dump_batch_size=cfg.collector.get("dump_batch_size"),
            gdrive_folder_id=cfg.storage.get("gdrive_folder_id"),
            local_folder=cfg.storage.get("local_folder"),
            gdrive=gdrive
        )
        