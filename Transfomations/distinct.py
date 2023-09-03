from pyspark import SparkContext

sc = SparkContext("local", "DistinctExample")
rdd = sc.parallelize([1, 2, 3, 4, 1, 2, 5, 6])

distinct_rdd = rdd.distinct()
print(distinct_rdd.collect())  # Returns [1, 2, 3, 4, 5, 6]

#in DF

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DistinctExample").getOrCreate()
data = [(1, "apple"), (2, "banana"), (3, "apple"), (4, "banana")]

df = spark.createDataFrame(data, ["id", "fruit"])

distinct_df = df.distinct()
distinct_df.show()
spark.stop()
