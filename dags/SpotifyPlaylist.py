import spotipy
import os
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from datetime import datetime, timedelta
import requests
import spotipy.util as util

def get_album_df(data):
    album_list = []
    
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        
        album_element = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                        'total_tracks': album_total_tracks, 'url': album_url}
        
        album_list.append(album_element)
        
    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['album_id'])
    album_df['release_date'] = pd.to_datetime(album_df['release_date'])
    
    return album_df


def get_artist_df(data):
    artist_list = []
    
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'], 'external_url': artist['href']}
                    artist_list.append(artist_dict)
    
    artist_df = pd.DataFrame.from_dict(artist_list)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])
    
    return artist_df    


def get_song_df(data):
    song_list = []

    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                            'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                            'artist_id':artist_id
                        }
        song_list.append(song_element)
        
    song_df = pd.DataFrame.from_dict(song_list)
    song_df['song_added'] = pd.to_datetime(song_df['song_added'])
    
    return song_df

def getdata():
    client_id = ''
    client_secret = ''

    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    playlist_link = 'https://open.spotify.com/playlist/37i9dQZF1DXc51TI5dx7RC'
    playlist_uri = playlist_link.split('/')[-1].split('?')[0]

    data = sp.playlist_tracks(playlist_uri)
        
    album_df = get_album_df(data)
    artist_df = get_artist_df(data)
    song_df = get_song_df(data)
        
    file_path = r"/home/airflow/data/" 
        
    album_df.to_csv(os.path.join(file_path, "album.csv"), index=False)
    artist_df.to_csv(os.path.join(file_path, "artist.csv"), index=False)
    song_df.to_csv(os.path.join(file_path, "song.csv"), index=False)

getdata()
