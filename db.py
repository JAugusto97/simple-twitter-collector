import mysql.connector

db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="eleicoes2022"
)

cursor = db.cursor()

cursor.execute(
  """CREATE TABLE IF NOT EXISTS tweets
  (
    tweet_id VARCHAR(80) PRIMARY KEY,
    user_id VARCHAR(80),
    news_id VARCHAR(80) PRIMARY KEY,
    text VARCHAR(360),
    created_at TIMESTAMP,
    geo VARCHAR(80),
    retweet_count INT UNSIGNED,
    reply_count INT UNSIGNED,
    like_count INT UNSIGNED,
    quote_count INT UNSIGNED,
    language VARCHAR(80),
    conversation_id VARCHAR(80),
    in_reply_to_user_id VARCHAR(80),
    media_url VARCHAR(360),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )""")

cursor.execute(
  """CREATE TABLE IF NOT EXISTS users
  (
    user_id VARCHAR(80) PRIMARY KEY,
    username VARCHAR(80),
    description VARCHAR(360),
    created_at TIMESTAMP,
    location VARCHAR(80),
    is_verified BOOLEAN,
    is_protected BOOLEAN,
    profile_image_url VARCHAR(360),
    followers_count INT UNSIGNED,
    following_count INT UNSIGNED,
    tweet_count INT UNSIGNED,
    listed_count INT UNSIGNED,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )""")
