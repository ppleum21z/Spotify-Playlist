from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pendulum
from SpotifyPlaylist import getdata
import os
from airflow.providers.mysql.operators.mysql import MySqlOperator

def read_sql_file(sql_file):
    with open(sql_file, 'r') as file:
        return file.read()
    
sql_file_dir = "/opt/airflow/sql/"

create_song_sql = read_sql_file(os.path.join(sql_file_dir, "CreateSongTable.sql"))
create_artist_sql = read_sql_file(os.path.join(sql_file_dir, "CreateArtistTable.sql"))
create_album_sql = read_sql_file(os.path.join(sql_file_dir, "CreateAlbumTable.sql"))
insert_song_sql = read_sql_file(os.path.join(sql_file_dir , "InsertSong.sql" ))
insert_artist_sql = read_sql_file(os.path.join(sql_file_dir , "InsertArtist.sql" ))
insert_album_sql = read_sql_file(os.path.join(sql_file_dir , "Insertalbum.sql" ))

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 11, 30),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id="My_Spotify_Data_Pipeline",
    schedule_interval="0 0 * * *",  # Daily schedule
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["spotify"]
) as dag:

    t1 = PythonOperator(
        task_id='get_data_from_spotify',
        python_callable=getdata
    )

    c1 = MySqlOperator(
        task_id='create_song_table',
        mysql_conn_id="mysql_con",
        sql=create_song_sql
    )

    c2 = MySqlOperator(
        task_id='create_album_table',
        mysql_conn_id='mysql_con', 
        sql=create_album_sql,
    )

    c3 = MySqlOperator(
        task_id='create_artist_table',
        mysql_conn_id='mysql_con', 
        sql=create_artist_sql,
    )

    i1 = MySqlOperator(
        task_id='insert_song_table',
        mysql_conn_id='mysql_con', 
        sql=insert_song_sql,
    )

    i2 = MySqlOperator(
        task_id='insert_album_table',
        mysql_conn_id='mysql_con', 
        sql=insert_album_sql,
    )

    i3 = MySqlOperator(
        task_id='insert_artist_table',
        mysql_conn_id='mysql_con', 
        sql=insert_artist_sql,
    )

t1 >> c1 >> i1
t1 >> c2 >> i2
t1 >> c3 >> i3



