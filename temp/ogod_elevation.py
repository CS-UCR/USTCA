from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, month, when
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import statsmodels.formula.api as smf
import pandas as pd

## GETTING DATA 
spark = SparkSession.builder.appName("ElevationAnalysis").getOrCreate()

rawObservationsDF = spark.read.csv("/home/cs179g/ustca(not github)/observations.csv/part-00000-0d699abc-208f-46d6-8620-59f4fd32a71a-c000.csv", header=True, inferSchema=True)
onlyTemp = rawObservationsDF.select('id', 'year', 'month', 'element', 'value').filter((rawObservationsDF['element'] == 'TMAX') | (rawObservationsDF['element'] == 'TMIN'))
observationsDF = onlyTemp.na.drop()
# print(onlyTemp.show(1))

rawStationsDF = spark.read.csv("/home/cs179g/USTCA/stations.csv", header=True, inferSchema=True)
onlyElevation = rawStationsDF.select('ID', 'ELEVATION', 'NAME')
stationsDF = onlyElevation.na.drop()
# print(stationsDF.show(10))


stationsDF = stationsDF.withColumnRenamed('ID', 'id')
finalDF = observationsDF.join(stationsDF, on='id')

# finalDF.select('value').summary().show()

finalDF = finalDF.withColumn(
    "elev_bin", 
    when (col('value') <= 0, "below")
    .when((col('value') > 0) & (col('value') <= 150), "low")
    .when((col('value') > 150) & (col('value') <= 300), "mid")
    .otherwise("high")
)

# finalDF.groupBy("elev_bin").count().orderBy("elev_bin").show()

finalTMax = finalDF.filter(finalDF['element'] == "TMAX")
# finalTMax.show(1)
finalTMin = finalDF.filter(finalDF['element'] == "TMIN")

avgTMaxByElevYear = finalTMax.groupBy("elev_bin", "year").agg(
    avg("value").alias("avg_temp")
)
# avgTMaxByElevYear.show()
avgTMinByElevYear = finalTMin.groupBy("elev_bin", "year").agg(
    avg("value").alias("avg_temp")
)

#######################
assembler = VectorAssembler(inputCols=['year'], outputCol='features')
avgTMaxFeatures = assembler.transform(avgTMaxByElevYear)

bins = avgTMaxFeatures.select("elev_bin").distinct().rdd.flatMap(lambda x:x).collect()

results =[]

for bin in bins: 
    binDF = avgTMaxFeatures.filter(avgTMaxFeatures['elev_bin'] == bin)

    lm = LinearRegression(featuresCol='features', labelCol="avg_temp")
    lm_fitted = lm.fit(binDF)

    slope = lm_fitted.coefficients[0]
    intercept = lm_fitted.intercept
    r2 = lm_fitted.summary.r2
    p_value = lm_fitted.summary.pValues[1] if lm_fitted.summary.pValues else None

    results.append((bin, float(slope), float(intercept), float(r2), float(p_value) if p_value is not None else None))

schema = StructType([
    StructField("elevation_bin", StringType(), True),
    StructField("slope_per_year", DoubleType(), True),
    StructField("intercept", DoubleType(), True),
    StructField("r_squared", DoubleType(), True),
    StructField("p_value", DoubleType(), True)
])

resultsDF = spark.createDataFrame(results, schema=schema)
resultsDF.orderBy("slope_per_year", ascending=False).show()
