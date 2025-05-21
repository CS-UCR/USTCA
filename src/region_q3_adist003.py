from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, substring, when, avg, sum, max, min, count, mean
from pyspark.sql.functions import to_date, make_date, year 
from pyspark.sql import Row
from pyspark.sql import udf
from pyspark.sql.types import StringType
import os
from pyspark.sql.window import Window
import statsmodels.formula.api as smf
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import scipy.stats as stats
import subprocess
import time


directory = 'data/region_analysis/test'
if not os.path.exists(directory):
    os.makedirs(directory)

spark = SparkSession.builder.appName("Region Analysis").getOrCreate()

df = spark.read.option('header','true').option('inferSchema','true').csv('/home/cs179g/USTCA/data/region_observations_samples/*.csv')

temp_df = df
temp_df = temp_df.withColumn('date', to_date(temp_df['date'], 'yy-MM-dd'))
temp_df = temp_df.withColumn('year', year(temp_df['date']))

extreme_weather_df = temp_df.filter(col('is_extreme_weather') == 'True')

region_df = extreme_weather_df.groupBy('region').agg(F.count('is_extreme_weather').alias('extreme_weather_count'))

region_pd = region_df.toPandas().sort_values('region')

plt.figure(figsize=(10, 6))
plt.bar(region_pd['region'], region_pd['extreme_weather_count'], color='skyblue')
plt.xlabel('Region')
plt.ylabel('Extreme Weather Count')
plt.title('Extreme Weather Events by Region')
plt.tight_layout()

plt.savefig(os.path.join(directory, 'extreme_weather_by_region.png'))
plt.close()
#4. Try each tasks using different s# spark workers
