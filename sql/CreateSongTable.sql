CREATE TABLE IF NOT EXISTS spotify.song
(song_id varchar(100),
 song_name varchar(100),
 duration_ms int,
 song_url varchar(300),
 popularity int,
 song_added time,
 album_id varchar(100),
 artist_id varchar(100),
PRIMARY KEY(song_id));
