LOAD DATA LOCAL INFILE '/home/airflow/data/artist.csv' 
INTO TABLE spotify.artist 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;