### WORK IN PROGRESS, DOES NOT WORK ON FULL DATASET YET. WORKS ON ONE FILE AT A TIME.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, substring, when, make_date
from pyspark.sql import Row

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()


# Read input file
file_path = "ghcnd_hcn" # uncomment for final bit
# file_path = "ghcnd_hcn/USC00011084.dly"

# df = spark.read.csv(file_path, header=False, inferSchema=True)
df = spark.read.option("inferSchema","true").csv(file_path) #.toDF(*col_names)
#     "id","year","month","element",
# 

#print(df.count())

cols = ['id', 'year', 'month', 'element']

day_columns = [f"value{i}" for i in range(1, 32)] + [f"mflag{i}" for i in range(1, 32)] + [f"qflag{i}" for i in range(1, 32)] + [f"slag{i}" for i in range(1, 32)]
col_names = cols + day_columns 
# df = df


# Extract and space-separate first few fields for demonstration
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


df = df_clean
# add colnames here


# Write to output file
# df.write.csv("cleaned_data.csv", header=True, mode="overwrite") # comment out

def removeNullAndFlags(df):

    #cleaning out rows with null in specified columns
    columns = ['ID', 'year', 'month','element']
    df_noNull = df.dropna(subset=columns)

    #removing all S and Q flag columns
    filtered = [c for c in df_noNull.columns if "sflag" not in c and "qflag" not in c]

    #selecting only the filtered columns
    df_final = df_noNull.select([col(c) for c in filtered])

    return df_final

df = removeNullAndFlags(df)

# Dataframe now has 66 cols
# 9: id year month element date
def toDaily(df):
    rows = []

    for row in df.collect():
        id = row['id']
        year = row['year']
        month = row['month']
        element = row['element']

        for day in range(1, 32):
            #creating column names based on day
            valueDay = f"value{day}"
            mflagDay = f"mflag{day}"

            #checking of value exists before creating the row
            if valueDay in row.asDict() and mflagDay in row.asDict():
                newValue = row[valueDay]
                newMflag = row[mflagDay]

                newRow = Row(id=id, year=year, month=month, element=element,
                        day=day, value=newValue, mflag=newMflag)
                #refers to the f-strings used earlier to rename and allocate values

            rows.append(newRow) #adding each day

    #atp, 'rows' has all days from all months cuz it converted all the rows in original df
    #combining all the rows into
    df_final = spark.createDataFrame(rows)

    return df_final

df = toDaily(df)
# 11

# made it to here
df = df.withColumn('value',
                when(col('value') <= -9999, None)
                .when((col('mflag').isNull()) | (col('mflag') == ''), None)
                .when(col('mflag') == 'P', 0)
                .otherwise(col('value'))
                 )

# 12
df = df.withColumn('value', col('value').cast('double'))
# df = df.withColumn('date', col('date').cast('int')) # changed to cast int
df = df.withColumn('element', col('element').cast('string'))
df = df.withColumn('id', col('id').cast('string'))

# convert month
df = df.withColumn('date', make_date('year', 'month', 'day'))
#13
df.coalesce(1).write.csv('observations.csv', header = True)

# Stop the spark job
spark.stop()
