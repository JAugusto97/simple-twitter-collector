from utils import DataBase, Config

if __name__ == "__main__":
    cfg = Config().database

    host = cfg.get("host")
    database = cfg.get("database")
    user = cfg.get("user")
    password = cfg.get("password")

    tables = cfg.get("tables")
    tweets = tables.get("tweets")
    media = tables.get("media")
    places = tables.get("places")
    users = tables.get("users")


    db = DataBase()
    db.connect(database="postgres", host=host, password=password, user=user)
    db.create_db(name=database)
    db.create_tables(tweets=tweets, media=media, places=places, users=users)