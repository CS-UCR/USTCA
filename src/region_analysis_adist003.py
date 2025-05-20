from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, substring, when, avg, sum, max, min, count, mean
from pyspark.sql.functions import to_date, make_date, year 
from pyspark.sql import Row
from pyspark.sql import udf
from pyspark.sql.types import StringType
import os
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Region Analysis").getOrCreate()

df = spark.read.option('header','true').option('inferSchema','true').csv('/home/cs179g/USTCA/data/region_observations/part-00067-2db47eea-2cb6-4d7b-85c8-67cec79039a9-c000.csv')

temp_df = df.filter(df['element']  == 'TMAX')
temp_df = temp_df.withColumn('date', to_date(temp_df['date'], 'yy-MM-dd'))
temp_df = temp_df.withColumn('year', year(temp_df['date']))

region_df = temp_df.groupBy('region', 'year').agg(F.avg('value').alias('avg_tmax'))

window_spec = Window.partitionBy('region').orderBy('year')
region_df = region_df.withColumn('prev_avg_tmax', F.lag('avg_tmax').over(window_spec))
region_df = region_df.withColumn('rate_of_change', (F.col('avg_tmax') - F.col('prev_avg_tmax')) / F.col('prev_avg_tmax'))

region_df = region_df.fillna({'prev_avg_tmax': 0})
region_df = region_df.fillna({'rate_of_change': 0})


region_df.show()