#!/bin/sh

DB_NAME="ustca"
TABLE_NAME="station"
STATION_FILE="data/stations.csv"

mysql --local-infile=1 $DB_NAME -e "
    LOAD DATA LOCAL INFILE '$STATION_FILE'
    INTO TABLE $TABLE_NAME
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (id, latitude, longitude, elevation, state, name);
"