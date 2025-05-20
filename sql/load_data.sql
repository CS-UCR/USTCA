USE ustca;

-- Drop, then create all tables
DROP TABLE IF EXISTS observation CASCADE;
DROP TABLE IF EXISTS station CASCADE;

-- Try to make the columns the smallest data types they can be

-- station:
--     id: 11 char, unique
--     latitude: decimal (look for range for size)
--     longitude: decimal (look for range for size)
--     elevation: decimal (look for range for size)
--     state: 2 char (example: California = CA)
--     name: haven't counted size yet, this would be varchar of at least 40-50

CREATE TABLE station (
    id CHAR(11) PRIMARY KEY,
    latitude DECIMAL(6, 4), 
    longitude DECIMAL(7, 4),
    elevation DECIMAL(7, 1), 
    state CHAR(2),          
    name VARCHAR(60)     
);

-- observation:
--     id: 11 char, unique (foreign key stationid)
--     date: Date (YYYYMMDD)
--     element: 4 char
--     data: Float/Double

CREATE TABLE observation (
    id CHAR(11),                   
    date DATE,
    element CHAR(4),
    data DOUBLE,
    FOREIGN KEY (id) REFERENCES station(id),
    UNIQUE (id, date, element)
);

-- load station
LOAD DATA LOCAL INFILE '/home/cs179g/ustca/stations.csv'
INTO TABLE station
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, latitude, longitude, elevation, state, name);


-- load observation
LOAD DATA LOCAL INFILE '/home/cs179g/ustca/observations.csv/part-00000-0d699abc-208f-46d6-8620-59f4fd32a71a-c000.csv'
INTO TABLE observation
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@id, @year, @month, @element, @day, @value, @mflag)
SET
  id = @id,
  date = STR_TO_DATE(CONCAT(@year, '-', LPAD(@month, 2, '0'), '-', LPAD(@day, 2, '0')), '%Y-%m-%d'),
  element = @element,
  data = COALESCE(NULLIF(@value, ''), NULL);