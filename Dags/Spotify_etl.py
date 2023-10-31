import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import pandas as pd
from datetime import datetime, timedelta

def get_recently_played():
    # Set up authorization
    client_id = '1511b062f1234c8183dd698a94a5832d'
    client_secret = '0065386d8a7d456599c086b39dc79985'
    username = '8ovqiixq95zs2vrh9qh1aemmw'
    scope = 'user-read-recently-played'
    redirect_uri = 'http://localhost:3000'
    
    # Get access token
    token = util.prompt_for_user_token(username, scope, client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)
    if not token:
        raise Exception("Unable to get token")

    # Create Spotify object
    sp = spotipy.Spotify(auth=token)

    # Get the current date and time
    now = datetime.now()

    # Calculate the time 24 hours ago
    today = datetime.now()
    yesterday = today - timedelta(days=2)
    timestamp_yesterday = int(yesterday.timestamp()) * 1000  # Convert to milliseconds

    # Retrieve recently played tracks after yesterday
    recently_played = sp.current_user_recently_played(after=timestamp_yesterday)

    # Extract relevant information from the response
    recently_played_data = []

    for item in recently_played['items']:
        track = item['track']
        song_length_minutes = track['duration_ms'] / (1000 * 60)  # Convert song length from milliseconds to minutes
        track_info = {
            'track_id': track['id'],
            'artist_name': track['artists'][0]['name'],
            'track_name': track['name'],
            'popularity': track['popularity'],
            'song_length_minutes': song_length_minutes
        }
        recently_played_data.append(track_info)

    print('Number of elements in the recently_played_data list:', len(recently_played_data))

    # Create DataFrame
    df_tracks = pd.DataFrame(recently_played_data)
    print(df_tracks.shape)
    return df_tracks

# Set of data quality checks needed to perform before loading
def data_quality(load_df):
    # Checking whether the DataFrame is empty
    if load_df.empty:
        print("No Data is extracted!")
        return False

    # Checking for null values in our data
    if load_df.isnull().values.any():
        print("Null values found!")
        raise Exception("Null values found")


    print("Data validation passed.")
    return True

def transform_df(load_df):
    # Drop duplicates based on 'track_name'
    transformed_df = load_df.drop_duplicates(subset=['artist_name', 'track_name'])

    return transformed_df[['track_id', 'track_name', 'artist_name', 'popularity', 'song_length_minutes']]

def spotify_etl():

    #importing the tracks_df form the extract.py
    load_df = get_recently_played()
    data_quality(load_df)

    #calling the transformations
    transformed_df = transform_df(load_df)
    print(load_df)
    return (load_df)
spotify_etl()