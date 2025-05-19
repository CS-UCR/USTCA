from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, substring, when, make_date
from pyspark.sql import Row
from pyspark.sql import udf
from pyspark.sql.types import StringType
import os
spark = SparkSession.builder.appName("Region Analysis").getOrCreate()

df = spark.read.option('header','true').option('inferSchema','true').csv('/home/cs179g/USTCA/region_observations/*.csv')

