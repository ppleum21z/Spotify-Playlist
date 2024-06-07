LOAD DATA LOCAL INFILE '/home/airflow/data/song.csv'
INTO TABLE spotify.song     
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
    