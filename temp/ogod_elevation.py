import pandas as pd 
#from sklearn.cluster import KMeans
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import year, month
import pyspark.sql.functions as F
import matplotlib
import matplotlib.pyplot as plt 
import seaborn as sns


# ## NOT SURE HOW EXACTLY HOW TO ACCESS SQL DB 
# sqlDB = pulling sql db 
# sql_query = """
#  SELECT S.id, S.elevation, O.date, O.element, O.data
#  FROM station S, observation O
#  WHERE S.id = O.id
#  """
# elevation_df = spark.sql(sql_query, sqlDB)

# # splitting elevation_df so one is for TMAX and other for TMIN 
# elevation_tmax_df = elevation_df[elevation_df["element"] == "TMAX"]
# elevation_tmin_df = elevation_df[elevation_df["element"] == "TMIN"]

###########################################################################################
spark = SparkSession.builder.appName("Elevation Analysis").getOrCreate()

rawStationsDF = spark.read.csv("/home/cs179g/ustca/stations.csv", header=True, inferSchema=True)
stationsDF = rawStationsDF.select('ID', 'ELEVATION')
stationsDF = stationsDF.withColumnRenamed('ID', 'id')

rawObservationsDF = spark.read.csv("/home/cs179g/ustca/src/observations.csv/part-00057-a54fb259-091e-4d8f-8cd7-1cff962c4753-c000.csv", header=True, inferSchema=True)
observationsDF = rawObservationsDF.select('id', 'date', 'element', 'value') \
                                .filter(col('element').isin(['TMAX', 'TMIN']))
observationsDF = observationsDF.withColumn('year', year(observationsDF['date']))
observationsDF = observationsDF.withColumn('month', month(observationsDF['date']))
observationsDF = observationsDF.drop('date')

elevation_df = observationsDF.join(stationsDF, on='id')

elevation_values = elevation_df.select('ELEVATION').toPandas()
print(elevation_values.describe())

# file_path = "ustca/src/observations.csv/part-00057-a54fb259-091e-4d8f-8cd7-1cff962c4753-c000.csv" #testing with one csv file
# #file_path = "ustca/src/observations.csv/*.csv"
# df = spark.read.csv(file_path, header=True, inferSchema=True)

# df_with_month = df.selectExpr(
#     "id",  # retain existing columns
#     "element",
#     "value",
#     "date",
#     "MONTH(date) AS month",  # extract month from 'date'
#     "YEAR(date) AS year" #extract year from date
######################################

# n = number of groups EDIT

# ## CHOOSE ONE SORTING TYPE BASED ON DATA SPREAD 
# # sorting out elevation in even percentile groups
# elevation_tmax_df['elevation_group'] = pd.qcut(elevation_tmax_df['elevation'], q=n, labels=['Low', 'Medium', 'High']) EDIT 

# # using kmeans to bin elevation 
# means = KMeans(n_clusters=n)
# elevation_tmax_df['elevation_group'] = kmeans.fit_predict(elevation_tmax_df[['elevation']])

# ## MAKE SURE TO COPY AGAIN FOR TMIN 
#######################################

# ## GETTING YEARLY AVERAGE FOR EACH ELEVATION GROUP 
# yearlyAvgTMAX = elevation_tmax_df.groupby(['elevation_group', 'year'])['data'].mean().reset_index()

# # Preview the grouped data
# print(yearlyAvgTMAX.head())

# ## MAKE SURE TO COPY AGAIN FOR TMIN
########################################
