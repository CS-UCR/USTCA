from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, month
import pandas
import mysql.connector

spark = SparkSession.builder.appName("SeasonalAnalysis").getOrCreate()


#file_path = "ustca/src/observations.csv/part-00057-a54fb259-091e-4d8f-8cd7-1cff962c4753-c000.csv" #testing with one csv file
file_path = "USTCA/data/observations/*.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

df_with_month = df.selectExpr(
    "id", 
    "element",
    "value",
    "date",
    "MONTH(date) AS month",  # extract month from date
    "YEAR(date) AS year" #extract year from date
    
)
#Add a "quarter" column
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

#Filter by element (TMAX, TMIN, PRCP)
#filtered = df_with_quarter.filter("element = 'TMAX' OR element = 'TMIN' OR element = 'PRCP'")
filtered = df_with_quarter.filter("element = 'TMIN'")
                                  
#Group by year, quarter, and element
quarterly_avg = filtered.groupBy("year", "quarter", "element").agg(
    avg("value").alias("average_value")
)

quarterly_avg = quarterly_avg.withColumn("average_value", col("average_value") / 10)

#Print results
#quarterly_avg.orderBy("year", "quarter", "element").show(50) #Single table with all elements 

#Separate table for each element
#tmax_table = quarterly_avg.filter("element = 'TMAX'").drop("element")
tmin_table = quarterly_avg.filter("element = 'TMIN'").drop("element")
#prcp_table = quarterly_avg.filter("element = 'PRCP'").drop("element")

#print("Average TMAX")
#tmax_table.orderBy("year", "quarter").show()
print("Average TMIN")
tmin_table.orderBy("year", "quarter").show()
#print("Average PRCP")
#prcp_table.orderBy("year", "quarter").show()

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

create_table_query = """
CREATE TABLE IF NOT EXISTS quarterly_averages (
    year INT,
    quarter VARCHAR(2),
    element VARCHAR(10),
    average_value FLOAT
)
"""
cursor.execute(create_table_query)
mydb.commit()

data = [
    (int(row['year']), row['quarter'], row['element'], float(row['average_value']))
    for _, row in pandas_df.iterrows()
]

insert_query = """
    INSERT INTO quarterly_averages (year, quarter, element, average_value)
    VALUES (%s, %s, %s, %s)
"""

cursor.executemany(insert_query, data)
mydb.commit()

cursor.close()
mydb.close()