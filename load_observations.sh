#!/bin/bash

DB_NAME="ustca"
TABLE_NAME="observation"
FOLDER="data/observations"


for file in "$FOLDER"/*.csv; do
    echo "Loading $file..."
    mysql --local-infile=1 $DB_NAME -e "
    LOAD DATA LOCAL INFILE '$file'
    INTO TABLE $TABLE_NAME
    FIELDS TERMINATED BY ',' 
      OPTIONALLY ENCLOSED BY '\"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@id, @element, @value, @date)
    SET
      id      = @id,
      element = @element,
      data    = NULLIF(@value, ''),
      date    = @date;
    "
done

mysql $DB_NAME -e "
DELETE FROM $TABLE_NAME where date = 0;
"