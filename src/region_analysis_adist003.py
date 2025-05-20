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
spark = SparkSession.builder.appName("Region Analysis").getOrCreate()
# for local: data/region_observations_sample
# for main machine: /home/cs179g/USTCA/data/region_observations_sample
df = spark.read.option('header','true').option('inferSchema','true').csv('/home/cs179g/USTCA/data/region_observations')

# 1 Is there a signficant diff between the average temp increase overtime per region?
# show that the regiosn differ in average temperature 
temp_df = df
temp_df = temp_df.withColumn('date', to_date(temp_df['date'], 'yy-MM-dd'))
temp_df = temp_df.withColumn('year', year(temp_df['date']))

tmax_df = temp_df.filter(temp_df['element']  == 'TMAX')

tmin_df = temp_df.filter(temp_df['element']  == 'TMIN')

tmin_region_df = temp_df.groupBy('region', 'year').agg(F.avg('value').alias('avg_tmin'))
tmax_region_df = temp_df.groupBy('region', 'year').agg(F.avg('value').alias('avg_tmax'))

window_spec = Window.partitionBy('region').orderBy('year')


tmin_region_df = tmin_region_df.withColumn('prev_avg_tmin', F.lag('avg_tmin').over(window_spec))
tmin_region_df = tmin_region_df.withColumn('rate_of_change', (F.col('avg_tmin') - F.col('prev_avg_tmin')) / F.col('prev_avg_tmin'))

tmax_region_df = tmax_region_df.withColumn('prev_avg_tmax', F.lag('avg_tmax').over(window_spec))
tmax_region_df = tmax_region_df.withColumn('rate_of_change', (F.col('avg_tmax') - F.col('prev_avg_tmax')) / F.col('prev_avg_tmax'))

tmin_region_df = tmin_region_df.fillna({'prev_avg_tmin': 0})
tmin_region_df = tmin_region_df.fillna({'rate_of_change': 0})

tmax_region_df = tmax_region_df.fillna({'prev_avg_tmax': 0})
tmax_region_df = tmax_region_df.fillna({'rate_of_change': 0})
#i probabaly want to graph region_df
# like graph by region (probably use a line graph to show all regiosn together)

# change_df = region_df.groupBy('region').agg(F.avg('rate_of_change').alias('average_roc'))

# change_df.show()

#do an anova
anovaMax = smf.ols('rate_of_change ~ region', data=tmax_region_df.toPandas()).fit()
print("ANOVA TMAX:")
print(anovaMax.summary())

anovaMin = smf.ols('rate_of_change ~ region', data=tmin_region_df.toPandas()).fit()
print("ANOVA TMIN:")
print(anovaMin.summary())
#2 What is the overall trend in temperature? and what is the trend in temperature per region?


# 3  Which Region has had the most extreme weather increases overtime
# Define extreme weather conditions
# do a group by to count tgem

# 3
# | Tool                          | Good For                  |
# | ----------------------------- | ------------------------- |
# | `pandas + matplotlib/seaborn` | Quick trend plotting      |
# | `statsmodels`                 | Time series decomposition |
# | `scikit-learn`                | Linear trend detection    |
# | `PySpark`                     | Scalable trend summaries  |
# | `Prophet`, `ARIMA`, `LSTM`    | Forecasting temperature   |

#4. Try each tasks using different s# spark workers

