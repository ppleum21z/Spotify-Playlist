CREATE TABLE IF NOT EXISTS spotify.album
(album_id varchar(100),
 album_name varchar(100),
 release_date date,
 total_tracks int,
 album_url varchar(300),

 PRIMARY KEY(album_id));
