import json
import gzip
import re
import boto3
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import sys
from datetime import datetime as dt, time
import datetime
import pandas as pd
import psycopg2
import sqlalchemy

from config import spotify_client_id, spotify_client_secret, dbname, db_password, S3_BUCKET, AWS_ACCESS_KEY_ID, SECRET_ACCESS_KEY

def extract_stage_data():
    '''
    Connects to Spotify API via spotipy and collects 50 most recent songs played
    Compresses data to gzip file then uploads in AWS S3 data lake
    '''

    spotify_redirect_url = 'http://localhost/'

    scope = 'user-read-recently-played'

    # Timestamp parameters
    today = dt.now()
    yday = today - datetime.timedelta(days=1)
    yday_unix_ts = int(yday.timestamp()) * 1000 # Round to int and muliply seconds to get milliseconds

    # Create timestamp variable for Airflow XCom. Variable used to pull same day S3 object for TL stages
    time_stamp = str(today).split('.')[0]
    time_stamp = re.sub(':', '.', time_stamp)


    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                client_secret=spotify_client_secret,
                                                redirect_uri=spotify_redirect_url,
                                                scope=scope))

    recently_played = sp.current_user_recently_played(limit=50, after=yday_unix_ts)

    # Convert recently_played to string
    recently_played_str = json.dumps(recently_played)
    recently_played_json = json.loads(recently_played_str)

    # Compress JSON object to gzip prior to S3 upload
    encoded_extract = recently_played_str.encode('utf-8')
    gz_file = gzip.compress(encoded_extract)
    
    # Create s3 client
    s3 = boto3.resource('s3', 
       aws_access_key_id=AWS_ACCESS_KEY_ID, 
       aws_secret_access_key=SECRET_ACCESS_KEY)

   # Create bucket/key object
    obj = s3.Object(S3_BUCKET, time_stamp + '/daily_songs_extract.gzip')

    # Upload object
    obj.put(Body=gz_file)

    print('Data extraction completed')
    print('Data uploaded to AWS S3 Bucket as gzip file\n--------')

    return time_stamp



def transform_data(ti):
    '''
    Function uses timestamp from XCom to load the corresponding gzip object in S3
    Gzip file is decompressed and transformed
    '''
    load_timestamp = ti.xcom_pull(key='return_value', task_ids='extract_stage_to_s3')
    print('Loaded timestamp:', load_timestamp)
    
    # Create s3 client
    s3 = boto3.resource('s3', 
        aws_access_key_id=AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=SECRET_ACCESS_KEY)

    # Create bucket/key object
    obj = s3.Object(S3_BUCKET, load_timestamp + '/daily_songs_extract.gzip')

    # Parse object to JSON then read, uses same object defined above
    s3_obj_data = obj.get()['Body'].read()
    data = gzip.decompress(s3_obj_data)
    data = data.decode('utf-8')
    data = json.loads(data)

    # Play information
    played_at = []
    timestamps = []

    # Song information
    song_ids = []
    song_names = []
    song_urls = []
    song_durations = []
    song_track_numbers = []
    song_popularity = []

    # Artist information
    artist_ids = []
    artist_names = []
    artist_urls = []

    # Album information
    album_ids = []
    album_names = []
    album_release_dates = []
    album_total_tracks = []
    album_urls = []


    # Extract information
    for record in data['items']:

        played_at.append(record['played_at'])
        timestamps.append(record['played_at'][0:10])

        song_ids.append(record['track']['id'])
        song_names.append(record['track']['name'])
        song_urls.append(record['track']['external_urls']['spotify'])
        song_durations.append(record['track']['duration_ms'])
        song_track_numbers.append(record['track']['track_number'])
        song_popularity.append(record['track']['popularity'])

        artist_ids.append(record['track']['artists'][0]['id'])
        artist_names.append(record['track']['artists'][0]['name'])
        artist_urls.append(record['track']['artists'][0]['external_urls']['spotify'])

        album_ids.append(record['track']['album']['id'])
        album_names.append(record['track']['album']['name'])
        album_release_dates.append(record['track']['album']['release_date'])
        album_total_tracks.append(record['track']['album']['total_tracks'])
        album_urls.append(record['track']['album']['external_urls']['spotify'])


    # Create table dictionaries: dict{list[]}
    song_plays = {
        'played_at': played_at,
        'song_id': song_ids,
        'artist_id': artist_ids,
        'timestamp': timestamps
    }

    dim_songs = {
        'song_id': song_ids,
        'song_name': song_names,
        'artist_id': artist_ids,
        'album_id': album_ids,
        'duration_ms': song_durations,
        'track_number': song_track_numbers,
        'popularity': song_popularity,
        'song_url': song_urls,
    }

    dim_artists = {
        'artist_id': artist_ids,
        'artist_name': artist_names,
        'artist_url': artist_urls
    }

    dim_albums = {
        'album_id': album_ids,
        'album_name': album_names,
        'artist_id': artist_ids,
        'release_date': album_release_dates,
        'total_tracks': album_total_tracks,
        'album_url': album_urls
    }

    song_plays_df = pd.DataFrame(song_plays, columns=song_plays.keys())
    
    dim_songs_df = pd.DataFrame(dim_songs, columns=dim_songs.keys())
    dim_songs_df.drop_duplicates(subset=['song_id'], inplace=True)

    dim_artists_df = pd.DataFrame(dim_artists, columns=dim_artists.keys())
    dim_artists_df.drop_duplicates(subset=['artist_id'], inplace=True)

    dim_albums_df = pd.DataFrame(dim_albums, columns=dim_albums.keys())
    dim_albums_df.drop_duplicates(subset=['album_id'], inplace=True)

    print('Data transformation completed\n--------')
    # print(dim_artists_df.head())
    # print(dim_albums_df[['album_id', 'album_name', 'artist_id']].head())
    return song_plays_df, dim_songs_df, dim_artists_df, dim_albums_df
    


def validate_data(song_plays_df, dim_songs_df, dim_artists_df, dim_albums_df):
    '''
    Performs data validation prior to loading into database
    Validates on: non-empty df, no null values, unique primary keys, date restriction
    '''
    df_list = [song_plays_df, dim_songs_df, dim_artists_df, dim_albums_df]
    
    for df in df_list:
    
        # Check if dataframe is empty
        if song_plays_df.empty:
            print('No songs downloaded. Finishing execution')
            return False
        
        # Null value check
        if song_plays_df.isnull().values.any():
            raise Exception('Null value found!')
    
    # Primary key check - no duplicates allowed! 'played_at' column is unique/PK
    if pd.Series(song_plays_df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary Key Check violation!')
 
    # Checking that all timestamps are of yesterday's date
    yday = dt.now() - datetime.timedelta(days=1)
    yday = yday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = song_plays_df['timestamp'].tolist()
    for stamp in timestamps:
        if dt.strptime(stamp, '%Y-%m-%d') < yday:
            print('Yesterday"', yday)
            print('Conflict datetime', dt.strptime(stamp, '%Y-%m-%d'))
            raise Exception('At least one of the returned songs does not come from within the last 24 hours')

    print('Data validation completed\n--------')

    return True



def load_data(song_plays_df, dim_songs_df, dim_artists_df, dim_albums_df): # Add dataframe parameters!
    '''
    Loads dataframes into PostgreSQL database using psycopg2 driver
    Need to insert in following order because of PK/FK constraints
        Artist -> Album -> Song -> Play Instance
    '''
    #Establish connections
    pg_conn = psycopg2.connect(
        dbname=dbname,
        user='postgres',
        password=db_password
        )
    pg_curr = pg_conn.cursor()

    alc_engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:babsy1995@localhost:5432/Spotify Data')
    alc_conn = alc_engine.raw_connection()
    alc_curr = alc_conn.cursor()
    

    insertion_dict = {
        'dim_artists': {
            'temp_table': 'artists_temp',
            'dataframe': dim_artists_df,
        },
        'dim_albums': {
            'temp_table': 'albums_temp',
            'dataframe': dim_albums_df
        },
        'dim_songs': {
            'temp_table': 'songs_temp',
            'dataframe': dim_songs_df
        },
        'song_plays': {
            'temp_table': 'plays_temp',
            'dataframe': song_plays_df
        }
    }


    def insert_data(insertion_dict):
        '''
        Loop insertion function to insert based on dictionary values
        '''
        for prod_table in insertion_dict:
            # Temp Table Creation via SQLAlchemy

            alc_curr.execute(
                f'''
                CREATE TEMP TABLE IF NOT EXISTS {insertion_dict[prod_table]["temp_table"]} as (
                SELECT *
                FROM spotify_schema.{prod_table}
                LIMIT 0
                );
                '''
            )

            insertion_dict[prod_table]['dataframe'].to_sql(f"{insertion_dict[prod_table]['temp_table']}", con=alc_engine, if_exists='append', index=False)

            pg_curr.execute(
                f'''
                INSERT INTO spotify_schema.{prod_table}(
                    SELECT {insertion_dict[prod_table]["temp_table"]}.*
                    FROM {insertion_dict[prod_table]["temp_table"]}
                )
                ON CONFLICT DO NOTHING;

                DROP TABLE {insertion_dict[prod_table]["temp_table"]};
                '''
            )
            pg_conn.commit()
            
    # Run insertion function
    insert_data(insertion_dict)

    print('Data loading completed\n--------')
    print("Songs inserted today:", len(pd.read_sql('select * from spotify_schema.dim_artists', pg_conn)))

    # Close db connections
    alc_conn.close()
    pg_conn.close()



def transform_validate_load_data(ti):
    '''
    Compiles transformation, validation, and loading functions into one function
    This is for ease of passing dataframes between function calls and makes orchestrating the DAG easier
    '''
    song_plays_df, dim_songs_df, dim_artists_df, dim_albums_df = transform_data(ti)
    if validate_data(song_plays_df, dim_songs_df, dim_artists_df, dim_albums_df):
        pass
    load_data(song_plays_df, dim_songs_df, dim_artists_df, dim_albums_df)

    print(f"Daily Spotify ETL Job Completed - {dt.now().strftime('%m/%d/%Y - %H:%M:%S')}")