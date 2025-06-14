from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import statsmodels.formula.api as smf
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from IPython.display import clear_output
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys
spark = SparkSession.builder.appName("ElevationAnalysis").config("spark.jars", "/home/cs179g/mysql-connector-j-9.3.0/mysql-connector-j-9.3.0.jar").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

def gettingData():
    query ="""
        SELECT id, elevation
        FROM station
        """
    rawStationsDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3307/ustca") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", f"({query}) as station") \
        .option("user", "root") \
        .option("password", "") \
        .load()

    stationsDF = rawStationsDF.na.drop()

    stationsDF = stationsDF.withColumn(
        "elev_bin", 
        when (col('elevation') <= 0, "below")
        .when((col('elevation') > 0) & (col('elevation') <= 150), "low")
        .when((col('elevation') > 150) & (col('elevation') <= 300), "mid")
        .otherwise("high")
    )

    query ="""
        SELECT id, YEAR(date) as date_key, tmin, tmax
        FROM observation
    """
    observationsDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3307/ustca") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", f"({query}) as observations") \
        .option("user", "root") \
        .option("password", "") \
        .option("partitionColumn", "date_key") \
        .option("lowerBound", "1925") \
        .option("upperBound", "2024") \
        .option("numPartitions", "16") \
        .load()


    observationsDF = observationsDF.withColumnRenamed("date_key", "year")

    finalDF = observationsDF.join(stationsDF, on='id')
    finalDF = finalDF.na.drop()

    finalTMax = finalDF.select("id", "elev_bin", "year", "tmax")

    finalTMin = finalDF.select("id", "elev_bin", "year", "tmin")

    avgTMaxByElevYear = finalTMax.groupBy("elev_bin", "year").agg(
        avg("tmax").alias("avg_temp")
    )

    avgTMinByElevYear = finalTMin.groupBy("elev_bin", "year").agg(
        avg("tmin").alias("avg_temp")
    )

    avgTMaxPD = avgTMaxByElevYear.toPandas() 
    avgTMinPD = avgTMinByElevYear.toPandas() 
    print("done getting data")
    return avgTMaxByElevYear, avgTMinByElevYear, avgTMaxPD, avgTMinPD

def runAnova(avgTMaxPD, avgTMinPD): 
    anovaMax = ols('avg_temp ~ C(elev_bin)', data=avgTMaxPD).fit()
    print("ANOVA (TMAX):")
    print(sm.stats.anova_lm(anovaMax, typ=2))
    
    tukeyMax = pairwise_tukeyhsd(endog=avgTMaxPD['avg_temp'],
                            groups=avgTMaxPD['elev_bin'],
                            alpha=0.05)
    print(tukeyMax)

    anovaMin = ols('avg_temp ~ C(elev_bin)', data=avgTMinPD).fit()
    print("ANOVA (TMIN):")
    print(sm.stats.anova_lm(anovaMin, typ=2))

    tukeyMin = pairwise_tukeyhsd(endog=avgTMinPD['avg_temp'],
                            groups=avgTMinPD['elev_bin'],
                            alpha=0.05)
    clear_output()
    print(tukeyMin)
    
    avg_tmax_by_bin = avgTMaxPD.groupby("elev_bin")["avg_temp"].mean()
    avg_tmin_by_bin = avgTMinPD.groupby("elev_bin")["avg_temp"].mean()

    print(f"{'Elevation Bin':<12} | {'Avg TMAX':>10} | {'Avg TMIN':>10}")
    print("-" * 38)

    for bin in sorted(avg_tmax_by_bin.index):
        tmax = avg_tmax_by_bin.get(bin, float('nan'))
        tmin = avg_tmin_by_bin.get(bin, float('nan'))
        print(f"{bin:<12} | {tmax:10.2f} | {tmin:10.2f}")

def runAnovaNoPrints(avgTMaxPD, avgTMinPD): 
    anovaMax = ols('avg_temp ~ C(elev_bin)', data=avgTMaxPD).fit()
    maxResults = sm.stats.anova_lm(anovaMax, typ=2)
    
    tukeyMax = pairwise_tukeyhsd(endog=avgTMaxPD['avg_temp'],
                            groups=avgTMaxPD['elev_bin'],
                            alpha=0.05)

    anovaMin = ols('avg_temp ~ C(elev_bin)', data=avgTMinPD).fit()
    minResults = sm.stats.anova_lm(anovaMin, typ=2)

    tukeyMin = pairwise_tukeyhsd(endog=avgTMinPD['avg_temp'],
                            groups=avgTMinPD['elev_bin'],
                            alpha=0.05)
    
def runLinearResults(avgTMax, avgTMin):
    assembler = VectorAssembler(inputCols=['year'], outputCol='features')
    avgTMaxFeatures = assembler.transform(avgTMax)
    avgTMinFeatures = assembler.transform(avgTMin)

    bins = avgTMaxFeatures.select("elev_bin").distinct().rdd.flatMap(lambda x:x).collect()

    resultsMax = []
    resultsMin = []

    for bin in bins: 
        binTMax = avgTMaxFeatures.filter(avgTMaxFeatures['elev_bin'] == bin)
        binTMin = avgTMinFeatures.filter(avgTMinFeatures['elev_bin'] == bin)

        lm = LinearRegression(featuresCol='features', labelCol="avg_temp")
        lm_TMax = lm.fit(binTMax)
        lm_TMin = lm.fit(binTMin)

        slope = lm_TMax.coefficients[0]
        intercept = lm_TMax.intercept
        r2 = lm_TMax.summary.r2
        p_value = lm_TMax.summary.pValues[1] if lm_TMax.summary.pValues else None

        slope1 = lm_TMin.coefficients[0]
        intercept1 = lm_TMin.intercept
        r21 = lm_TMin.summary.r2
        
        try:
            if lm_TMin.summary.pValues and len(lm_TMin.summary.pValues) > 1:
                p_value1 = float(lm_TMin.summary.pValues[1])
            else:
                p_value1 = None
        except Exception as e:
            p_value1 = None
            
        resultsMax.append((bin, float(slope), float(intercept), float(r2), float(p_value) if p_value is not None else None))
        resultsMin.append((bin, float(slope1), float(intercept1), float(r21), float(p_value1) if p_value1 is not None else None))
    return resultsMax, resultsMin

def runLinear(avgTMax, avgTMin):
    resultsMax, resultsMin = runLinearResults(avgTMax, avgTMin)
    
    schema = StructType([
        StructField("elevation_bin", StringType(), True),
        StructField("slope_per_year", DoubleType(), True),
        StructField("intercept", DoubleType(), True),
        StructField("r_squared", DoubleType(), True),
        StructField("p_value", DoubleType(), True)
    ])

    resultsMax = spark.createDataFrame(resultsMax, schema=schema)
    print("Linear Model (TMAX):")
    print(resultsMax.orderBy("slope_per_year", ascending=False).show())
    
    resultsMin = spark.createDataFrame(resultsMin, schema=schema)
    print("Linear Model (TMIN):")
    print(resultsMin.orderBy("slope_per_year", ascending=False).show())

def runLinearNoPrints(avgTMax, avgTMin):
    resultsMax, resultsMin = runLinearResults(avgTMax, avgTMin)
    
    schema = StructType([
        StructField("elevation_bin", StringType(), True),
        StructField("slope_per_year", DoubleType(), True),
        StructField("intercept", DoubleType(), True),
        StructField("r_squared", DoubleType(), True),
        StructField("p_value", DoubleType(), True)
    ])

    resultsMax = spark.createDataFrame(resultsMax, schema=schema)
    maxOutput = resultsMax.orderBy("slope_per_year", ascending=False)
    
    resultsMin = spark.createDataFrame(resultsMin, schema=schema)
    minOutput = resultsMin.orderBy("slope_per_year", ascending=False)
    
def runAncova(avgTMaxPD, avgTMinPD): 

    ancovaMax = smf.ols('avg_temp ~ year * C(elev_bin)', data=avgTMaxPD).fit()
    print("ANCOVA (TMAX):")
    print(ancovaMax.summary())

    ancovaMin = smf.ols('avg_temp ~ year * C(elev_bin)', data=avgTMinPD).fit()
    print("ANCOVA (TMIN):")
    print(ancovaMin.summary())

def runAncovaNoPrints(avgTMaxPD, avgTMinPD): 

    ancovaMax = smf.ols('avg_temp ~ year * C(elev_bin)', data=avgTMaxPD).fit()
    maxSummary = ancovaMax.summary()

    ancovaMin = smf.ols('avg_temp ~ year * C(elev_bin)', data=avgTMinPD).fit()
    minSummary = ancovaMin.summary()

avgTMaxByElevYear, avgTMinByElevYear, avgTMaxPD, avgTMinPD = gettingData()
originalSTDout = sys.stdout
with open('outputs/elevation/elevationAnalysis.txt', 'w') as f:
    sys.stdout = f

    print("1. DO THE ELEVATION BINS HAVE DIFFERENT AVERAGE TEMPERATURES?\n")
    runAnova(avgTMaxPD, avgTMinPD)

    print("\n\n\n 2. WHAT TRENDS DO EACH OF THE ELEVATION BINS HAVE?\n")
    runLinear(avgTMaxByElevYear, avgTMinByElevYear)

    print("\n\n\n 3. WHICH ELEVATION BIN CHANGED THE MOST?\n")
    runAncova(avgTMaxPD, avgTMinPD)

sys.stdout = originalSTDout

sns.lmplot(data=avgTMaxPD, x="year", y="avg_temp", hue="elev_bin", aspect=2)
plt.title("Average Max Temperature Trends by Elevation Bin")
plt.savefig("outputs/elevation/avgTempByYearsAcrossBinsMax.png", dpi=300, bbox_inches='tight')

sns.lmplot(data=avgTMinPD, x="year", y="avg_temp", hue="elev_bin", aspect=2)
plt.title("Average Min Temperature Trends by Elevation Bin")
plt.savefig("outputs/elevation/avgTempByYearsAcrossBinsMin.png", dpi=300, bbox_inches='tight')

resultsMax, resultsMin = runLinearResults(avgTMaxByElevYear, avgTMinByElevYear)
    
slopesDF = pd.DataFrame({
    "elev_bin": [r[0] for r in resultsMax],
    "TMAX_slope": [r[1] for r in resultsMax],
    "TMIN_slope": [r[1] for r in resultsMin]
})

slopesDF['elev_bin'] = pd.Categorical(
    slopesDF['elev_bin'],
    categories=['below', 'low', 'mid', 'high'],
    ordered=True
)

slopesDF.set_index("elev_bin")[["TMAX_slope", "TMIN_slope"]].plot(kind='bar', figsize=(10,6))
plt.title("Temperature Trend Slopes per Elevation Bin")
plt.ylabel("Slope (Temperature Change per Year)")
plt.xlabel("Elevation Bin")
plt.axhline(0, color='gray', linestyle='--')
plt.savefig("outputs/elevation/rateofChangeTempAcrossBins.png", dpi=300, bbox_inches='tight')


stationCountsMax = avgTMaxByElevYear.groupBy("elev_bin").count()
stationCountsMaxPD = stationCountsMax.toPandas()

plt.figure(figsize=(6, 6))
plt.pie(
    stationCountsMaxPD['count'], 
    labels=stationCountsMaxPD['elev_bin'],
    autopct='%1.1f%%',
    startangle=140
)
plt.title("Station Count by Elevation Bin")
plt.axis('equal')
plt.savefig("outputs/elevation/pieStationCount.png", dpi=300, bbox_inches='tight')


