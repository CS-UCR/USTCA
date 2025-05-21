from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import os
import math

spark = SparkSession.builder.appName("Region and Extreme Weather Classification").getOrCreate()

file_path = "/home/cs179g/USTCA/data/observations/*.csv"
stations_path = '/home/cs179g/USTCA/data/stations.csv'

main_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
stations_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(stations_path)

stations_df = stations_df.withColumnRenamed('ID', 'station_id') \
                         .withColumn("LATITUDE", col("LATITUDE").cast("double")) \
                         .withColumn("LONGITUDE", col("LONGITUDE").cast("double"))

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

joined_df = joined_df.withColumn('region', classify_region_udf(col('LONGITUDE'), col('LATITUDE')))

def is_extreme_weather(value_name, value):
    if value is None or value_name is None:
        return "False"
    value = float(value)
    value_name = str(value_name)
    if value_name == 'TMAX':
        value = value / 10
        return "True" if value > 35 else "False"
    elif value_name == 'TMIN':
        value = value / 10
        return "True" if value < -18 else "False"
    elif value_name == 'SNWD':
        return "True" if value > 500 else "False"
    elif value_name == 'SNOW':
        return "True" if value > 250 else "False"
    elif value_name == 'PRCP':
        return "True" if value > 50 else "False"
    else:
        return "False"

classify_extreme_weather = F.udf(is_extreme_weather, StringType())

joined_df = joined_df.withColumn('is_extreme_weather', classify_extreme_weather(col('element'), col('value')))

joined_df.show()

joined_df.write.csv('region_observations', header=True, mode='overwrite')

spark.stop()
