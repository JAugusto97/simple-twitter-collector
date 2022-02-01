# Simple Tweet Collector
A simple tweet collector wrapper built on top of tweepy and optionally integrated with Google Drive and PostgreSQL. It will collect all the tweet, user, media and location fields and dump it to a json file. You can optionally upload your raw files into a Google Drive and, also optionally, parse them into a PostgreSQL database.

## 🏃 How to Run
1. Install the dependencies:  
```pip3 install -r requirements.txt```
2. Follow these [instructions](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api) to get your Twitter API keys. Paste your Twitter API keys inside **credentials/twitter_credentials.json** 
3. (Optional) Follow these [instructions](https://developers.google.com/drive/api/v3/about-auth#OAuth2Authorizing) to get your Google Drive OAuth 2.0 secret keys. Download the **client_secrets.json** file and place it into **credentials/client_secrets.json**
4. (Optional) Install and configure a PostgreSQL database for your specific OS.
5. Modify the **config.yaml** file with your desired configurations.
6. To collect the tweets:  
   ```make collect```
7. (Optional) To parse the raw tweets into a PostgreSQL database:
  ```make parse```

## 🔧 Configuration File
You can modify the **config.yaml** file to suit your needs. Some fields are optional if you want to upload your files to Google Drive and insert them into PostgreSQL. Below you will find the description of the fields you can modify. Required fields are in bold.

- credentials
  - **twitter_credentials** (str): path to your Twitter API credentials file.
  - google_drive_credentials (str, optional): path to your Google Drive API credentials file.
  - **is_twitter_elevated_access** (bool): true if you have elevated access to Twitter API, false otherwhise. If you don't have elevated access, you can only collect tweets from up to 1 week ago, so 'start_date' and 'end_date' don't need to be specified.
- storage
  - gdrive_folder_id (str, optional): Google Drive folder ID where your tweets will be dumped into. To get this, simply create or access the desired folder inside your Google Drive and copy the url after "/folders/": https://drive.google.com/drive/u/1/folders/<folder_id>
  - **dump_to_google_drive** (bool): true if you want to upload your tweets to Google Drive (must set other related parameters) or false if you want to save them locally.
  - **local_folder** (str): a local folder path to save the tweets into.
- collector
  - **task_id** (str): your tweets will be saved inside a folder with this name, either locally or on Google Drive. It does not override the folder if it already exists. You can use this to keep track of multiple tweet collection tasks.
  - **query** (str): will collect the tweets mentioning this string. You can create more sophisticated Twitter queries that suit your needs using this [documentation](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query). 
  - max_results (int, optional): collect up to this number of tweets. If null, will collect everything.
  - dump_batch_size (int, optional): dump a batch of this number of tweets. If null, will dump a single file containing every tweet. 
  - start_time (string, optional): string timestamp to collect tweets starting from this period of time. If null, will collect tweets starting from any period of time. Format is "yyyy-mm-ddThh-mm-ssZ"
  - end_time (string, optional): string timestamp to collect tweets up to this period of time. If null, will collect tweets up to any period of time. Format is "yyyy-mm-ddThh-mm-ssZ". end_time must always be a timestamp before than your current timestamp.
  - recent (bool, optional): if you have elevated access, you can collect tweets both from the full archive of tweets and from recent (up to 7 days) tweets. If you have default access, you can only collect recent tweets.
- database
  - host (str, optional): postgresql connection host
    database (str, optional): postgresql database name
    user (str, optional): postgresql user
    password (str, optional): postgresql password
    tables:
        tweets (bool, optional): true
        users (bool, optional): true
        media (bool, optional): true
        places (bool, optional): false

## 🔍 Tweet Fields Retrieved
These are the data fields that will be collected. You can find more details [here](https://developer.twitter.com/en/docs/twitter-api/fields).
| tweet               | author            | media             | place            |
|---------------------|-------------------|-------------------|------------------|
| text                | id                | key               | id               |
| id                  | name              | type              | full_name        |
| created_at          | username          | duration_ms       | contained_within |
| public_metrics      | created_at        | height            | country          |
| in_reply_to_user_id | description       | width             | geo              |
| conversation_id     | entities          | preview_image_url | name             |
| lang                | location          | alt_text          | type             |
|                     | is_protected      | view_count        |                  |
|                     | is_verified       |                   |                  |
|                     | profile_image_url |                   |                  |
|                     | public_metrics    |                   |                  |
## ❗ Elevated Access to Twitter API
If your API keys have default access, some of the fields retrieved might always be empty. Also, if you don't have elevated access, you can only collect tweets from up to 1 week ago. If you're a researcher, you can ask Twitter for elevated access [here](https://developer.twitter.com/en/products/twitter-api/academic-research).
