#!/bin/bash

DB_USER="root"
DB_NAME="ustca"
DB_PORT=3307
TABLE="region_observations"
DIR="ustca/region_db.sh"

TABLE_EXISTS=$(mysql -u $DB_USER --port=$DB_PORT -D $DB_NAME -se "SHOW TABLES LIKE '$TABLE';")

if [ "$TABLE_EXISTS" != "$TABLE" ]; then
    mysql -u $DB_USER --port=$DB_PORT $DB_NAME -e "
        DROP TABLE IF EXISTS $TABLE;

        CREATE TABLE $TABLE (
            id CHAR(11),
            element CHAR(4),
            value FLOAT,
            date DATE,
            station_id CHAR(11),
            latitude DECIMAL(6,4),
            longitude DECIMAL(7,4),
            elevation DECIMAL(7,1),
            state CHAR(2),
            name VARCHAR(60),
            region VARCHAR(20),
            is_extreme_weather BOOLEAN,
            PRIMARY KEY (id, date, element)
        );

        CREATE INDEX idx_region_element ON $TABLE(region, element);
        CREATE INDEX idx_element ON $TABLE(element);
        CREATE INDEX idx_date ON $TABLE(date);
        CREATE INDEX idx_station_id ON $TABLE(station_id);
    "
fi

for file in "$DIR"/*.csv
do
  mysql -u $DB_USER --port=$DB_PORT $DB_NAME --local-infile=1 -e "
    LOAD DATA LOCAL INFILE '$file'
    INTO TABLE $TABLE
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@id, @element, @value, @date, @station_id, @latitude, @longitude, @elevation, @state, @name, @region, @is_extreme_weather)
    SET
      id = IF(@element IN ('TMAX', 'TMIN', 'PRCP'), @id, NULL),
      element = @element,
      value = @value,
      date = @date,
      station_id = @station_id,
      latitude = @latitude,
      longitude = @longitude,
      elevation = @elevation,
      state = @state,
      name = @name,
      region = @region,
      is_extreme_weather = @is_extreme_weather;
  "

  mysql -u $DB_USER --port=$DB_PORT $DB_NAME -e "
    DELETE FROM $TABLE WHERE id IS NULL;
  "
done
