from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, substring, when, make_date
from pyspark.sql import Row
from pyspark.sql import udf
from pyspark.sql.types import StringType, BinaryType
import os
spark = SparkSession.builder.appName("Extreme Weather Classification").getOrCreate()


file_path = "/home/cs179g/USTCA/data/region_observations_sample/*.csv"

main_df = spark.read.format("csv").option("header","true").load(file_path)
def is_extreme_weather(value_name, value):
    try:
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
    except:
        return "False"

classify_extreme_weather = F.udf(is_extreme_weather, StringType())

weather_df = main_df.withColumn('is_extreme_weather', classify_extreme_weather(col('element'), col('value')))

weather_df.filter(col('is_extreme_weather') == 'True').show()

weather_df.write.csv('data/extreme_weather_observations', header=True, mode='overwrite')

spark.stop()