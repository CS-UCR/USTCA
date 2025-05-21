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

tmax_df = temp_df.filter(temp_df['element']  == 'TMAX')
tmin_df = temp_df.filter(temp_df['element']  == 'TMIN')

tmin_region_df = temp_df.groupBy('region', 'year').agg(F.avg('value').alias('avg_tmin'))
tmax_region_df = temp_df.groupBy('region', 'year').agg(F.avg('value').alias('avg_tmax'))

window_spec = Window.partitionBy('region').orderBy('year')

# 1 Is there a signficant diff between the average temp increase overtime per region?
# show that the regiosn differ in average temperature 
tmin_region_df = tmin_region_df.withColumn('prev_avg_tmin', F.lag('avg_tmin').over(window_spec))
tmin_region_df = tmin_region_df.withColumn('rate_of_change', (F.col('avg_tmin') - F.col('prev_avg_tmin')) / F.col('prev_avg_tmin'))

tmax_region_df = tmax_region_df.withColumn('prev_avg_tmax', F.lag('avg_tmax').over(window_spec))
tmax_region_df = tmax_region_df.withColumn('rate_of_change', (F.col('avg_tmax') - F.col('prev_avg_tmax')) / F.col('prev_avg_tmax'))

tmin_region_df = tmin_region_df.fillna({'prev_avg_tmin': 0})
tmin_region_df = tmin_region_df.fillna({'rate_of_change': 0})

tmax_region_df = tmax_region_df.fillna({'prev_avg_tmax': 0})
tmax_region_df = tmax_region_df.fillna({'rate_of_change': 0})

#2 What is the overall trend in temperature? and what is the trend in temperature per region?
tmin_region_pd = tmin_region_df.toPandas()
tmax_region_pd = tmax_region_df.toPandas()

# For the TMIN plot
plt.figure(figsize=(12, 6))
sns.lmplot(data=tmin_region_pd, x='year', y='avg_tmin', hue='region', scatter_kws={'s': 100},
           line_kws={'linewidth': 2, 'linestyle': '--'}, ci=None, palette='Set1', robust=True)
plt.title('TMIN vs Year by Region with Linear Regression Line')
plt.xlabel('Year')
plt.ylabel('Average TMIN')
plt.tight_layout()
plt.savefig(os.path.join(directory, 'scatterplot_tmin_region.png'))
plt.close()

# For the TMAX plot
plt.figure(figsize=(12, 6))
sns.lmplot(data=tmax_region_pd, x='year', y='avg_tmax', hue='region', scatter_kws={'s': 100},
           line_kws={'linewidth': 2, 'linestyle': '-.'}, ci=None, palette='Set2', robust=True)
plt.title('TMAX vs Year by Region with Linear Regression Line')
plt.xlabel('Year')
plt.ylabel('Average TMAX')
plt.tight_layout()
plt.savefig(os.path.join(directory, 'scatterplot_tmax_region.png'))
plt.close()