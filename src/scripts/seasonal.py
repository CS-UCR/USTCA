from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, month
import pandas
import mysql.connector

spark = SparkSession.builder.appName("SeasonalAnalysis").getOrCreate()


file_path = "../../data/observations/*.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

df_with_month = df.selectExpr(
    "id", 
    "element",
    "value",
    "date",
    "MONTH(date) AS month",
    "YEAR(date) AS year"
    
)
df_with_quarter = df_with_month.selectExpr(
    "*", 
    """
    CASE 
        WHEN month IN (1, 2, 3) THEN 'Q1'
        WHEN month IN (4, 5, 6) THEN 'Q2'
        WHEN month IN (7, 8, 9) THEN 'Q3'
        WHEN month IN (10, 11, 12) THEN 'Q4'
    END AS quarter
    """
)

quarterly_avg = df_with_quarter.groupBy("id", "year", "quarter", "element").agg(
    avg("value").alias("average_value")
)

quarterly_avg = quarterly_avg.withColumn("average_value", col("average_value") / 10)

print("Average TMAX")
tmax_table = quarterly_avg.filter("element = 'TMAX'").show()
print("Average TMIN")
tmin_table = quarterly_avg.filter("element = 'TMIN'").show()
print("Average TMIN")
prcp_table = quarterly_avg.filter("element = 'PRCP'").show()

pandas_df = quarterly_avg.toPandas()
pandas_df = pandas_df.dropna()

mydb = mysql.connector.connect(
    host="localhost",
    port="3307",
    user="root",
    password="",
    database="ustca"
)

cursor = mydb.cursor()

cursor.execute("DROP TABLE IF EXISTS quarterly_averages")

create_table_query = """
CREATE TABLE IF NOT EXISTS quarterly_averages (
    id VARCHAR(11),
    year INT,
    quarter VARCHAR(2),
    element VARCHAR(10),
    average_value FLOAT
)
"""
cursor.execute(create_table_query)
mydb.commit()

data = [
    (row['id'], int(row['year']), row['quarter'], row['element'], float(row['average_value']))
    for _, row in pandas_df.iterrows()
]


insert_query = """
    INSERT INTO quarterly_averages (id, year, quarter, element, average_value)
    VALUES (%s, %s, %s, %s, %s)
"""

batch_size = 1000
for i in range(0, len(data), batch_size):
    cursor.executemany(insert_query, data[i:i+batch_size])
    mydb.commit()

cursor.close()
mydb.close()