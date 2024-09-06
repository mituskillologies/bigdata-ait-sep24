from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[4]').appName('mapreduce').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
rdd = sc.parallelize([56,23,12,36,56,77])

increments = rdd.map(lambda x: x + 5)
print('After Mapping:', increments.collect())

from operator import add
red = rdd.reduce(add)
print('After Filtering:', red)

rdd1 = sc.parallelize([("spark", 1), ("hadoop", 4)])
rdd2 = sc.parallelize([("spark", 3), ("hadoop", 5)])
rdd3 = rdd1.join(rdd2)
print('After Joining:', rdd3.collect())
