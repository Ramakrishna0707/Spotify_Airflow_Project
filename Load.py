import Extract
import Transform
import sqlalchemy
import pandas as pd

# Define your MySQL connection parameters
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = '2002'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3306'
MYSQL_DB_NAME = 'Spotify_db'

# Create a MySQL connection string
DATABASE_LOCATION = f"mysql+mysqlconnector://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"

if __name__ == "__main__":
    # Importing the df_tracks from the extract.py
    load_df = Extract.get_recently_played()
    if not Transform.data_quality(load_df):
        raise Exception("Failed at Data Validation")
    
    transformed_df = Transform.transform_df(load_df)

    # The two data frames that need to be loaded into the database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = engine.connect()

    # SQL query to create a tracks table
    sql_query_1 = """
    CREATE TABLE IF NOT EXISTS songs(
        track_id VARCHAR(200),
        artist_name VARCHAR(200),
        track_name VARCHAR(200),
        popularity INT,
        song_length_minutes DECIMAL
    )
    """
    # SQL query to create Unique_songs table
    sql_query_2 = """
    CREATE TABLE IF NOT EXISTS unique_songs(
        track_id VARCHAR(200),
        artist_name VARCHAR(200),
        track_name VARCHAR(200),
        popularity INT,
        song_length_minutes DECIMAL,
        CONSTRAINT primary_key_constraint PRIMARY KEY(track_id)
    )
    """
    conn.execute(sqlalchemy.text(sql_query_1))
    conn.execute(sqlalchemy.text(sql_query_2))

    # We need to append only new data to avoid duplicates
    try:
        load_df.to_sql('songs', engine, index=False, if_exists='append')
    except Exception as e:
        print("Error: ", str(e))
        print("Data already exists in the database")

    try:
        transformed_df.to_sql('unique_songs', engine, index=False, if_exists='append')
    except Exception as e:
        print("Error: ", str(e))
        print("Data already exists in the database2")

    conn.close()
    print("Close database successfully")
