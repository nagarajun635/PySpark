from pyspark import SparkContext

sc = SparkContext("local", "SortByKeyExample")
rdd = sc.parallelize([(3, "apple"), (1, "banana"), (2, "cherry")])

sorted_rdd_ascending = rdd.sortByKey(ascending=True)
print(sorted_rdd_ascending.getNumPartitions())
sorted_rdd_descending = rdd.sortByKey(ascending=False,numPartitions=2)
print(sorted_rdd_descending.getNumPartitions())

print(sorted_rdd_ascending.collect())
print(sorted_rdd_descending.collect())

#in DF

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SortByKeyExample").getOrCreate()
data = [(3, "apple"), (1, "banana"), (2, "cherry")]

df = spark.createDataFrame(data, ["key", "value"])

sorted_df_ascending = df.orderBy("key")
sorted_df_descending = df.orderBy("key", ascending=False)

sorted_df_ascending.show()
sorted_df_descending.show()
