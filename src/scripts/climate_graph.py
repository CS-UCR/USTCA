from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, sum as _sum
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("ClimateGraph") \
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

climate = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3307/ustca") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "climate") \
    .option("user", "root") \
    .option("password", "") \
    .load()

query ="""
    SELECT *, TO_DAYS(date) as date_key
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

stations = stations.join(climate, on='id')
observations_with_year = observations.withColumn("year", year("date"))

joined = observations_with_year.join(stations, on="id")

df = joined.groupBy("class", "id", "year").agg(
    avg("TMAX").alias("TMAX"),
    avg("TMIN").alias("TMIN"),
    _sum("PRCP").alias("PRCP")
).orderBy("id", "year")

pandas_df = df.toPandas()
elements = ['TMAX', 'TMIN', 'PRCP']
climates = ['A', 'B', 'C', 'D']
y_ranges = {
    'TMAX': (-100, 500),
    'TMIN': (-100, 500),
    'PRCP': (0, 40000)
}

for i, elem in enumerate(elements):
    for j, zone in enumerate(climates):
        sub = pandas_df[pandas_df['class'] == zone][['year', elem]]

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.scatter(sub['year'], sub[elem])
        ax.set_title(f"{elem} - Zone {zone}")
        ax.set_xlabel("Year")
        ax.set_ylabel("Value")
        ax.grid(True)
        ax.set_ylim(y_ranges[elem])

        filename = f"outputs/climate_{elem}_zone_{zone}.png"
        plt.tight_layout()
        plt.savefig(filename)
        plt.close(fig) 