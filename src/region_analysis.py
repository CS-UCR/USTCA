from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, substring, when, make_date
from pyspark.sql import Row
from pyspark.sql import udf
from pyspark.sql.types import StringType
import os
spark = SparkSession.builder.appName("Region Analysis").getOrCreate()


# Read input file
#file_path = "ghcnd_hcn" # uncomment for final bit
file_path = "/home/cs179g/ustca/ghcn/observations.csv"
# file_path = "ghcnd_hcn/USC00011084.dly"

# df = spark.read.csv(file_path, header=False, inferSchema=True)
main_df = spark.read.format("csv").option("header","true").load(file_path)
#     "id","year","month","element",
# 

stations_path = 'stations.csv'
stations_df = spark.read.format('csv').option('header','true').load(stations_path)
#print(df.count())

# NW = North West
# W = West 
# SW = South West
# MW = Mid-West
# SE = South East
# MA = Mid-Atlantic
# NE = North East

def classify_region(lat, lon):
    if lat > 40 and lon < -120:
        return 'NW'
    elif lat > 30 and -120 <= lon < -100:
        return 'W'
    elif lat < 40 and -120 <= lon < -100:
        return 'SW'
    elif lat > 40 and -100 <= lon < -85:
        return 'MW'
    elif lat < 40 and -85 <= lon < -70:
        return 'SE'
    elif 36 <= lat <= 42 and -80 <= lon < -70:
        return 'MA'
    elif lat > 40 and lon > -75:
        return 'NE'
    else:
        return 'Unknown'

classify_region_udf = udf(classify_region, StringType())

joined_df = main_df.join(stations_df, main_df['id'] == stations_df['ID'], 'left')

joined_df = joined_df.withColumn('region', classify_region_udf(stations_df['LATITUDE'], stations_df['LONGITUDE']))     

joined_df

# Stop the spark job
spark.stop()
