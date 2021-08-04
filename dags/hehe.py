import gzip
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime as dt
import datetime
from config import spotify_client_id, spotify_client_secret, dbname, db_password, S3_BUCKET, AWS_ACCESS_KEY_ID, SECRET_ACCESS_KEY
import boto3
import json
import re

spotify_redirect_url = 'http://localhost/'

scope = 'user-read-recently-played'

# Timestamp parameters
today = dt.now()
yday = today - datetime.timedelta(days=1)
yday_unix_ts = int(yday.timestamp()) * 1000 # Round to int and muliply seconds to get milliseconds


sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                            client_secret=spotify_client_secret,
                                            redirect_uri=spotify_redirect_url,
                                            scope=scope))

recently_played = sp.current_user_recently_played(limit=50, after=yday_unix_ts)

# Get most recently played song
sp_str = json.dumps(recently_played['items'][0])
sp_json = json.loads(sp_str)

# print(sp_str)

print(sp_json['track']['name'])
print(sp_json['track']['artists'][0]['name'])

t = sp_str.encode('utf-8')
gz = gzip.compress(t)

decomp = gzip.decompress(gz)
t = decomp.decode('utf-8')

t_json = json.loads(t)

print(t_json['track']['name'])
print(t_json['track']['artists'][0]['name'])