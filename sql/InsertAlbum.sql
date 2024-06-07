LOAD DATA LOCAL INFILE '/home/airflow/data/album.csv' 
INTO TABLE spotify.album 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;