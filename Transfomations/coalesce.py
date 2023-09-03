from pyspark import SparkContext

sc = SparkContext("local", "CoalesceExample")
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4)  # Create an RDD with 4 partitions
print(rdd.getNumPartitions())
coalesced_rdd = rdd.coalesce(2)
print(coalesced_rdd.getNumPartitions())

#in DF
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CoalesceExample").getOrCreate()
data = [(1, "apple"), (2, "banana"), (3, "cherry"), (4, "date"), (5, "grape")]
columns = ["id", "fruit"]
df = spark.createDataFrame(data, columns)
print(df.rdd.getNumPartitions())
coalesced_df = df.coalesce(2)
print(coalesced_df.rdd.getNumPartitions())
