from pyspark import SparkContext

sc = SparkContext("local", "RepartitionExample")
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],10)
print(rdd.getNumPartitions())
repartitioned_rdd = rdd.repartition(4)
print(repartitioned_rdd.getNumPartitions())
#in DF
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RepartitionExample").getOrCreate()
data = [(1, "apple"), (2, "banana"), (3, "cherry"), (4, "date"), (5, "grape")]
columns = ["id", "fruit"]

df = spark.createDataFrame(data, columns)
print(df.rdd.getNumPartitions())
repartitioned_df = df.repartition(3)
print(repartitioned_df.rdd.getNumPartitions())
repartitioned_df.show()

