from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, month

spark = SparkSession.builder.appName("SeasonalAnalysis").getOrCreate()


#file_path = "ustca/src/observations.csv/part-00057-a54fb259-091e-4d8f-8cd7-1cff962c4753-c000.csv" #testing with one csv file
file_path = "ustca/src/observations.csv/*.csv"
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
filtered = df_with_quarter.filter("element = 'TMAX' OR element = 'TMIN' OR element = 'PRCP'")

#Group by year, quarter, and element
quarterly_avg = filtered.groupBy("year", "quarter", "element").agg(
    avg("value").alias("average_value")
)

quarterly_avg = quarterly_avg.withColumn("average_value", col("average_value") / 10)

#Print results
#quarterly_avg.orderBy("year", "quarter", "element").show(50) #Single table with all elements 

#Separate table for each element
tmax_table = quarterly_avg.filter("element = 'TMAX'").drop("element")
tmin_table = quarterly_avg.filter("element = 'TMIN'").drop("element")
prcp_table = quarterly_avg.filter("element = 'PRCP'").drop("element")

print("Average TMAX")
tmax_table.orderBy("year", "quarter").show()
print("Average TMIN")
tmin_table.orderBy("year", "quarter").show()
print("Average PRCP")
prcp_table.orderBy("year", "quarter").show()

#MYSQL??
#mysql_url = "jdbc:mysql://localhost:3306/your_db_name"
#mysql_properties = {
# "user": "your_username",
# "password": "your_password",
# "driver": "com.mysql.cj.jdbc.Driver"
#}

#quarterly_avg.write.jdbc(
#   url=mysql_url,
#   table="quarterly_averages",
#   mode="overwrite", 
#   properties=mysql_properties
#)