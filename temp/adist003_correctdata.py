from pyspark.sql.functions import when, col

df = df.withColumn('value',
                when(col('value') <= -9999, None)
                .when((col('mflag').isNull()) | (col('mflag') == ''), None)
                .when(col('mflag') == 'P', 0)
                .otherwise(col('value'))
                 )

df = df.withColumn('value', col('value').cast('double'))
df = df.withColumn('date', col('date').cast('date'))
df = df.withColumn('element', col('element').cast('string'))
df = df.withColumn('id', col('id').cast('string'))
