from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, avg, max, min, when, first, udf, sum
from pyspark.sql.types import BooleanType


spark = SparkSession.builder \
    .appName("ClimateAnalysis") \
    .config("spark.jars", "/home/cs179g/mysql-connector-j-9.3.0/mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

stations = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3307/ustca") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "station") \
    .option("user", "root") \
    .option("password", "") \
    .load()

query ="""
    SELECT id, date, tmin, tmax, prcp, TO_DAYS(date) as date_key
    FROM observation
"""
observations = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3307/ustca") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", f"({query}) as observations") \
    .option("user", "root") \
    .option("password", "") \
    .option("partitionColumn", "date_key") \
    .option("lowerBound", "675454") \
    .option("upperBound", "739719") \
    .option("numPartitions", "16") \
    .load()

data = observations.withColumn(
    "tavg",
    when(col("tmin").isNull() & col("tmax").isNull(), None)
    .when(col("tmin").isNull(), col("tmax"))
    .when(col("tmax").isNull(), col("tmin"))
    .otherwise((col("tmin") + col("tmax")) / 2)
) \
    .withColumn('month',month('date')) \
    .groupBy('id','month') \
    .agg(avg('tavg').alias('avg_temp'), avg('prcp').alias('avg_prcp')) \
    .orderBy('id','month')

data = data.withColumn(
    "season",
    when(col("month").between(4, 9), "summer").otherwise("winter")
)

seasonal_data = data.groupBy("id", "season").agg(
    sum("avg_prcp").alias("season_prcp"),
    avg("avg_temp").alias("season_temp")
)
piv_prcp = seasonal_data.groupBy("id").pivot("season").agg(first("season_prcp")) \
    .withColumnRenamed("summer", "p_summer") \
    .withColumnRenamed("winter", "p_winter")
piv_temp = seasonal_data.groupBy("id").pivot("season").agg(first("season_temp")) \
    .withColumnRenamed("summer", "t_summer") \
    .withColumnRenamed("winter", "t_winter")

seasonal_joined = piv_prcp.join(piv_temp, on="id")

def is_dry_climate(avgtemp, mean_prcp, p_summer, p_winter):

    p_ann = mean_prcp * 12
    mean_temp = avgtemp/10

    if p_summer > 2 * p_winter:
        p_thresh = 20 * mean_temp + 280
    elif p_winter > 2 * p_summer:
        p_thresh = 20 * mean_temp + 140
    else:
        p_thresh = 20 * mean_temp + 70

    return p_ann < p_thresh

is_dry_udf = udf(is_dry_climate, BooleanType())

data = data.groupBy("id").agg(
    avg("avg_temp").alias("mean_temp"),
    avg("avg_prcp").alias("mean_prcp"),
    max("avg_temp").alias("max_temp"),
    min("avg_temp").alias("min_temp"),
    min("avg_prcp").alias("min_prcp")
) \
    .join(seasonal_joined, on='id')

data = data.withColumn(
    "is_dry",
    is_dry_udf(
        col("mean_temp"),
        col("mean_prcp"),
        col("p_summer"),
        col("p_winter")
    )
)

data = data.withColumn(
    "climate",
    when(col("mean_temp") >= 180, "A") \
    .when(col("is_dry") == True, "B") \
    .when((col("t_summer") >= 100) & (col("t_winter") >= -30) & (col("t_winter") <= 180), "C")
    .when((col("min_temp") <= -30) & (col("max_temp") >= 100), "D")
    .otherwise("Unclassified")
)
class_stations = stations.join(data.select("id","climate"), on="id")
del data, seasonal_joined, piv_prcp, piv_temp, seasonal_data

climate_stations = class_stations.select('id','climate')
climate_stations.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3307/ustca") \
    .option("dbtable", "climate") \
    .option("user", "root") \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()

