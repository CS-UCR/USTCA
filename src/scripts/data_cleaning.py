
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, when, make_date

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

file_path = "../../data/ghcnd_hcn"

df = spark.read.option("inferSchema","true").csv(file_path)


cols = ['id', 'year', 'month', 'element']

day_columns = [f"value{i}" for i in range(1, 32)] + [f"mflag{i}" for i in range(1, 32)] + [f"qflag{i}" for i in range(1, 32)] + [f"slag{i}" for i in range(1, 32)]
col_names = cols + day_columns 


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

def removeNullAndFlags(df):

    columns = ['ID', 'year', 'month','element']
    df_noNull = df.dropna(subset=columns)

    filtered = [c for c in df_noNull.columns if "sflag" not in c and "qflag" not in c]

    df_final = df_noNull.select([col(c) for c in filtered])

    return df_final

df = removeNullAndFlags(df)

def toDaily(df):
    day_expr = ", ".join([
        f"{day}, value{day}, mflag{day}" for day in range(1, 32)
    ])
    
    stacked = df.selectExpr("id", "year", "month", "element",
        f"stack(31, {day_expr}) as (day, value, mflag)"
    )
    
    return stacked
df = toDaily(df)

df = df.withColumn('value',
                when(col('value') <= -9999, None)
                .when((col('mflag').isNull()) | (col('mflag') == ''), None)
                .when(col('mflag') == 'P', 0)
                .otherwise(col('value'))
                 )

df = df.withColumn('value', col('value').cast('double'))
df = df.withColumn('element', col('element').cast('string'))
df = df.withColumn('id', col('id').cast('string'))

df = df.withColumn('date', make_date('year', 'month', 'day'))

df = df.drop('mflag', 'year', 'month', 'day')

df = df.filter(df.element.isin('PRCP','TMAX','TMIN'))

df.write.csv('observations.csv', header=True, mode='overwrite')

spark.stop()