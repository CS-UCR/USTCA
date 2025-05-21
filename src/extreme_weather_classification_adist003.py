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
    if value is None:
        return False
    value = float(value)
    value_name = str(value_name)
    
    if value_name == 'TMAX':
        value = value/10
        if value  > 35: # 35 C
            return True
        else:
            return False
        
    elif value_name == 'TMIN':
        value = value/10
        if value < -18: # 18 C
            return True
        else:
            return False

    elif value_name == 'SNWD':
        if value > 500: # 500nm (20 inches)
            return True
        else:
            return False
    elif value_name == 'SNOW':
        if value > 250: # 250nm (10 inches)
            return True
        else:
            return False
    elif value_name == 'PRCP':
        if value > 50: # 50nm (2 inches)
            return True
        else:
            return False
    return None



classify_extreme_weather = F.udf(main_df, StringType())

weather_df = main_df.withColumn('is_extreme_weather', classify_extreme_weather(main_df['element'], main_df['value']))     

weather_df.show()

weather_df.write.csv('extreme_weather_observations.csv', header=True, mode='overwrite')

# Stop the spark job
spark.stop()
