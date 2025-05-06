from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("SparkTest").getOrCreate()

nums = sc.parallelize([1, 2, 3])
squares = nums.map(lambda x: x*x)
even = squares.filter(lambda x: x % 2 == 0)
even.collect()
x = sc.parallelize(["spark rdd example", "sample example"])
y = x.flatMap(lambda x: x.split(' '))
y.collect()

sc.stop()


# Read input file
# Split into columns based on README
