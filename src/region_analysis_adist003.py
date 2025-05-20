from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, substring, when, avg, sum, max, min, count, mean
from pyspark.sql.functions import to_date, make_date, year 
from pyspark.sql import Row
from pyspark.sql import udf
from pyspark.sql.types import StringType
import os
spark = SparkSession.builder.appName("Region Analysis").getOrCreate()

df = spark.read.option('header','true').option('inferSchema','true').csv('/home/cs179g/USTCA/data/region_observations/part-00067-2db47eea-2cb6-4d7b-85c8-67cec79039a9-c000.csv')

#df = df.withColumn()

temp_df = df.filter(df['value']  == 'TMAX')
temp_df = temp_df.withColumn('date', to_date(temp_df['date'], 'yy-MM-dd'))
temp_df = temp_df.withColumn('year', year(temp_df['date']))

region_df = temp_df.groupBy('region', 'year').agg(F.avg('value').alias('avg_TMAX'))
region_df.show()