# United States Temperature and Climate Analysis
#### CS179G - Project in Databases
### Created by Josh Pennington, Yixuan Shang, Ojasvi Godha, Adreyan Distor and Salma Ahmed

In this project, we aim to analyze weather data across the United States using Global Historical Climatology Network – Daily (GHCN-Daily) dataset. We wish to explore which major area of the US has been most noticeably getting warmer throughout the years. 

Our dataset includes daily records from various weather stations, including maximum/minimum temperatures, precise geolocation metadata (latitude, longitude, and elevation) and timestamped entries dating back over a century. 

We plan to divide the weather stations, via their longitudes and latitudes, to split the US into 3 regions: West, Central, and East. We will analyze any noticeable trends in each region to determine which has the highest warming trend and we believe the West region will be the most affected. 

Additionally, using our analysis, we plan to develop a linear regression model using Apache Spark to predict daily weather measurements based on three features: Longitude, Latitude and Date. This model will allow us to estimate the temperature for any locations in the US with date, based on spatial and temporal patterns in historical data.


## About the Dataset

Our main dataset we use is a subset of the Global Historical Climatology Network. Specifically, we are only using part of the United States weather data. The detailed README file for GHCN can be found [here](https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt). We were able to reduce the given raw dataset into four columns: ID, DATE, ELEMENT, and VALUE. The format and definitions of those are as follows:

ID          11 characters. The station identification code which can be linked to the station in "stations.txt".

DATE        8 characters. The date of when the record was recorded (YYYYMMDD).

ELEMENT     4 characters. The three types of elements we are using are as follows:

            PRCP = Precipitation (tenths of mm)
            TMAX = Maximum temperature (tenths of degrees C)
            TMIN = Minimum temperature (tenths of degrees C)

VALUE       Integer. The recorded value of the element on this particular day.

## How to run the code:
System requirements: (list)
spark-submit
