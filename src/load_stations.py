from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark
spark = SparkSession.builder.appName("load_stations").getOrCreate() 

# Read input file
# spark_text = spark.read.text("ghcnd-stations.txt")

# Defining the textfile's schema as outlined in the README
station_schema = [(0,11),(12,20),(21,30),(31,37),(38,40),(41,71),(72,75),(76,79),(80,85)]
col_names = ["ID","LATITUDE","LONGITUDE","ELEVATION","STATE","NAME","GSN FLAG","HCN/CRN FLAG","WMO ID"]

# Read the input file and filter based on the schema
df = pd.read_fwf("ghcnd-stations.txt", colspecs=station_schema, names=col_names)

# Keep relevant columns
df = df[['ID','LATITUDE','LONGITUDE','ELEVATION','STATE','NAME']]

# Keep only US stations
# df = df.dropna(subset=["STATE"])
# df = df.loc[~df['ID'].str.contains('US'),:]
mask = df['ID'].str.contains('US')
df = df[mask]

df.to_csv('stations.csv', mode='w', index=False)

spark.stop()