import psycopg2

class DataBase:
    def __init__(self):
        self.conn = None
        self.tables = []

    def connect(self, host, database, user, password):
        try:
            self.conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def close(self):
        if self.conn:
            self.conn.close()
    
    def __del__(self):
        self.close()

    def execute(self, command):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(command)

    def create_tables(
        self,
        tweets=True,
        users=True,
        media=True,
        places=True
    ):   
        if tweets:
            self.tables.append("tweets")
            self.execute(
            """CREATE TABLE IF NOT EXISTS tweets
            (
                tweet_id VARCHAR(80) NOT NULL,
                user_id VARCHAR(80),
                task_id VARCHAR(80) NOT NULL,
                text VARCHAR(1000),
                created_at TIMESTAMP,
                place_id VARCHAR(80),
                retweet_count INT,
                reply_count INT,
                like_count INT,
                quote_count INT,
                language VARCHAR(80),
                conversation_id VARCHAR(80),
                in_reply_to_user_id VARCHAR(80),
                media_key VARCHAR(360),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(tweet_id, task_id)
            )"""
            )
        if users:
            self.tables.append("users")
            self.execute(
            """CREATE TABLE IF NOT EXISTS users
            (
                user_id VARCHAR(80) PRIMARY KEY,
                username VARCHAR(80),
                description VARCHAR(1000),
                created_at TIMESTAMP,
                location VARCHAR(1000),
                is_verified BOOLEAN,
                is_protected BOOLEAN,
                profile_image_url VARCHAR(360),
                followers_count INT,
                following_count INT,
                tweet_count INT,
                listed_count INT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
            )
        if media:
            self.tables.append("media")
            self.execute(
            """
            CREATE TABLE IF NOT EXISTS media
            (
                media_key VARCHAR(360) NOT NULL,
                type VARCHAR(160),
                duration_ms INT,
                height INT,
                width INT,
                preview_image_url VARCHAR(360),
                alt_text VARCHAR(1000),
                view_count INT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(media_key)
            )
            """
            )
        if places:
            self.tables.append("places")
            self.execute(
            """
            CREATE TABLE IF NOT EXISTS places
            (
                place_id VARCHAR(360) NOT NULL,
                full_name VARCHAR(1000),
                country VARCHAR(300),
                name VARCHAR(300),
                type VARCHAR(300),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(place_id)
            )
            """
            )
    
    def insert_batch(self, table_name, data, pkeys):
        cols = ", ".join(data[0].keys())
        values = "".join([f"%({key})s," for key in data[0].keys()])[:-1]
        pkeys = ", ".join(pkeys)
        sql = \
            f"""
            INSERT INTO {table_name} ({cols})
            VALUES ({values}) ON CONFLICT ({pkeys}) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
            """

        cur = self.conn.cursor()
        cur.executemany(sql, data)
        self.conn.commit()

    def insert():
        pass
