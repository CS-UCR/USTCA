from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, substring, when, make_date
from pyspark.sql import Row
from pyspark.sql import udf
from pyspark.sql.types import StringType
import os
import math

spark = SparkSession.builder.appName("Region Delineation").getOrCreate()

curr_wcd = os.getcwd()
# Read input file
#file_path = "ghcnd_hcn" # uncomment for final bit
file_path = "/home/cs179g/USTCA/data/observations/*.csv"
# file_path = "ghcnd_hcn/USC00011084.dly"

# df = spark.read.csv(file_path, header=False, inferSchema=True)
main_df = spark.read.format("csv").option("header","true").load(file_path)
#     "id","year","month","element",
# 

stations_path = '/home/cs179g/USTCA/data/stations.csv'
stations_df = spark.read.format('csv').option('header','true').load(stations_path)

stations_df = stations_df.withColumnRenamed('ID', 'station_id') 

def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def classify_region(lon, lat):
    lon = float(lon)
    lat = float(lat)
    region_centers = {
        'Northeast': (-74.0, 41.5),
        'Midwest': (-87.5, 41.6),
        'South': (-84.4, 33.0),
        'West': (-118.2, 34.0)
    }
    distances = {
        region: haversine(lon, lat, center_lon, center_lat)
        for region, (center_lon, center_lat) in region_centers.items()
    }
    return min(distances, key=distances.get)



classify_region_udf = F.udf(classify_region, StringType())

joined_df = main_df.join(stations_df, main_df['id'] == stations_df['station_id'], 'left')

joined_df = joined_df.withColumn('region', classify_region_udf(joined_df['LATITUDE'], joined_df['LONGITUDE']))     

joined_df.show()

joined_df.write.csv('region_observations', header=True, mode='overwrite')

# Stop the spark job
spark.stop()
