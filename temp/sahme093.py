from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()


# Read input file
file_path = "ghcnd_hcn"
df = spark.read.csv(file_path, header=False, inferSchema=True)

from pyspark.sql.functions import col, substring

# Extract and space-separate first few fields for demonstration
#df = df.select(
    #F.concat_ws(" ",
     #   F.substring('_c0', 1, 11),
      #  F.substring('_c0', 12, 4),
       # F.substring('_c0', 16, 2),
        #F.substring('_c0', 18, 4),
        #*[F.substring('_c0', 22 + i * 8, 5) for i in range(31)]
    #).alias("space_separated")
#)

base_fields = [
    substring('_c0', 1, 11).alias('id'),
    substring('_c0', 12, 4).alias('year'),
    substring('_c0', 16, 2).alias('month'),
    substring('_c0', 18, 4).alias('element'),
]

daily_fields = []
for i in range(31):
    start = 22 + i * 8
    daily_fields += [
        substring('_c0', start, 5).cast("int").alias(f'value{i+1}'),
        substring('_c0', start + 5, 1).alias(f'mflag{i+1}'),
        substring('_c0', start + 6, 1).alias(f'qflag{i+1}'),
        substring('_c0', start + 7, 1).alias(f'sflag{i+1}'),
    ]

df_clean = df.select(*(base_fields + daily_fields))

df_clean.printSchema()
df_clean.show(5, truncate=False)

# Write to output file
df_clean.write.csv("cleaned_data.csv", header=True, mode="overwrite")

