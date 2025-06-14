USE ustca;

-- load station
LOAD DATA LOCAL INFILE '../../data/stations.csv'
INTO TABLE station
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, latitude, longitude, elevation, state, name);