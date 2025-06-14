from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("load_stations").getOrCreate() 

station_schema = [(0,11),(12,20),(21,30),(31,37),(38,40),(41,71),(72,75),(76,79),(80,85)]
col_names = ["ID","LATITUDE","LONGITUDE","ELEVATION","STATE","NAME","GSN FLAG","HCN/CRN FLAG","WMO ID"]

df = pd.read_fwf("ghcnd-stations.txt", colspecs=station_schema, names=col_names)

df = df[['ID','LATITUDE','LONGITUDE','ELEVATION','STATE','NAME']]

mask = df['ID'].str.contains('US')
df = df[mask]

df.to_csv('stations.csv', mode='w', index=False)

spark.stop()