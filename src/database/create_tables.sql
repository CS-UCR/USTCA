-- create database for storing tables
CREATE DATABASE IF NOT EXISTS ustca;
USE ustca;

-- Drop, then create all tables
DROP TABLE IF EXISTS observation CASCADE;
DROP TABLE IF EXISTS station CASCADE;

-- Station
CREATE TABLE station (
    id CHAR(11) PRIMARY KEY,
    latitude DECIMAL(6, 4), 
    longitude DECIMAL(7, 4),
    elevation DECIMAL(7, 1), 
    state CHAR(2),          
    name VARCHAR(60)     
);

-- Observation
CREATE TABLE observation (
    id CHAR(11),                   
    date DATE,
    element CHAR(4),
    data DOUBLE,
    FOREIGN KEY (id) REFERENCES station(id),
    UNIQUE (id, date, element)
);

--Quarterly Averages
CREATE TABLE IF NOT EXISTS quarterly_averages (
    id VARCHAR(11),
    year INT,
    quarter VARCHAR(2),
    element VARCHAR(10),
    average_value FLOAT
);

-- Region Observations
CREATE TABLE IF NOT EXISTS region_observations (
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

-- Region Summary Stats
CREATE TABLE IF NOT EXISTS region_summary_stats (
    region varchar(20),
    avg_tmin DOUBLE,
    min_tmin DOUBLE,
    max_tmin DOUBLE,
    avg_tmax DOUBLE,
    min_tmax DOUBLE,
    max_tmax DOUBLE,
    avg_prcp DOUBLE,
    min_prcp DOUBLE,
    max_prcp DOUBLE
);

-- Regional Averages
CREATE TABLE IF NOT EXISTS regional_averages (
    id CHAR(11),
    state CHAR(2),
    element CHAR(4),
    avg_value double,
    year int,
    region varchar(20)
);

-- Climate
CREATE TABLE IF NOT EXISTS Climate (
    id CHAR(11),
    class CHAR(1)
    PRIMARY KEY (id, class)
);