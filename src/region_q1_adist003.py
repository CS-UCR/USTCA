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

anovaMax = smf.ols('rate_of_change ~ region', data=tmax_region_df.toPandas()).fit()
anovaMax_summary = anovaMax.summary()

anovaMax_df = pd.DataFrame(anovaMax_summary.tables[1])
anovaMax_df.to_csv(os.path.join(directory, 'anovaMax_results.csv'))

anovaMin = smf.ols('rate_of_change ~ region', data=tmin_region_df.toPandas()).fit()
anovaMin_summary = anovaMin.summary()

anovaMin_df = pd.DataFrame(anovaMin_summary.tables[1])
anovaMin_df.to_csv(os.path.join(directory, 'anovaMin_results.csv'))

resid_max = anovaMax.resid
resid_min = anovaMin.resid

plt.figure(figsize=(12, 6))

plt.subplot(1, 2, 1)
sns.histplot(resid_max, kde=True)
plt.title('Residuals Distribution for TMAX')

plt.subplot(1, 2, 2)
sns.histplot(resid_min, kde=True)
plt.title('Residuals Distribution for TMIN')

plt.tight_layout()
plt.savefig(os.path.join(directory, 'residuals_distribution.png'))
plt.close()

plt.figure()
stats.probplot(resid_max, dist='norm', plot=plt)
plt.title('Q-Q Plot for TMAX Residuals')
plt.savefig(os.path.join(directory, 'qq_plot_tmax.png'))
plt.close()

plt.figure()
stats.probplot(resid_min, dist='norm', plot=plt)
plt.title('Q-Q Plot for TMIN Residuals')
plt.savefig(os.path.join(directory, 'qq_plot_tmin.png'))
plt.close()

plt.figure(figsize=(12, 6))

plt.subplot(1, 2, 1)
plt.scatter(anovaMax.fittedvalues, resid_max)
plt.axhline(0, color='red', linestyle='--')
plt.title('Residuals vs Fitted for TMAX')
plt.xlabel('Fitted values')
plt.ylabel('Residuals')

plt.subplot(1, 2, 2)
plt.scatter(anovaMin.fittedvalues, resid_min)
plt.axhline(0, color='red', linestyle='--')
plt.title('Residuals vs Fitted for TMIN')
plt.xlabel('Fitted values')
plt.ylabel('Residuals')

plt.tight_layout()
plt.savefig(os.path.join(directory, 'residuals_vs_fitted.png'))
plt.close()

print("ANOVA TMAX Results Saved to CSV:")
print(anovaMax_summary)
print("\nANOVA TMIN Results Saved to CSV:")
print(anovaMin_summary)