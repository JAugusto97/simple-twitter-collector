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
    tweet_id VARCHAR(80) NOT NULL,
    user_id VARCHAR(80),
    news_id VARCHAR(80) NOT NULL,
    text VARCHAR(1000),
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
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(tweet_id, news_id)
  )"""
)

cursor.execute(
  """CREATE TABLE IF NOT EXISTS users
  (
    user_id VARCHAR(80) PRIMARY KEY,
    username VARCHAR(80),
    description VARCHAR(360),
    created_at TIMESTAMP,
    location VARCHAR(360),
    is_verified BOOLEAN,
    is_protected BOOLEAN,
    profile_image_url VARCHAR(360),
    followers_count INT UNSIGNED,
    following_count INT UNSIGNED,
    tweet_count INT UNSIGNED,
    listed_count INT UNSIGNED,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )"""
)

cursor.execute(
  """
  CREATE TABLE IF NOT EXISTS key_users
  (
    user_id VARCHAR(80) PRIMARY KEY,
    username VARCHAR(80)
  )
  """
)

cursor.execute(
  """
  CREATE TABLE IF NOT EXISTS fake_news
  (
    news_id VARCHAR(360),
    source VARCHAR(360),
    headline VARCHAR(1000),
    url VARCHAR(580),
    created_at TIMESTAMP,
    search_query VARCHAR(1000),
    prejudices_candidacy VARCHAR(360),
    PRIMARY KEY(news_id, source)
  )
  """
)