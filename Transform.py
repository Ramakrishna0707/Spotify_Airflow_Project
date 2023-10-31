import Extract
import pandas as pd

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


if __name__ == "__main__":
    # Importing the df_tracks from the extract.py
    load_df = Extract.get_recently_played()
    data_quality(load_df)

    # Calling the transformation
    transformed_df = transform_df(load_df)
    print(transformed_df)