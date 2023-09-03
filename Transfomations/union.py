from pyspark import SparkContext

sc = SparkContext("local", "UnionExample")
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4, 5, 6])

union_rdd = rdd1.union(rdd2)
print(union_rdd.collect())  # Returns [1, 2, 3, 4, 3, 4, 5, 6]

#in DF

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UnionExample").getOrCreate()
data1 = [(1, "apple"), (2, "banana")]
data2 = [(3, "cherry"), (4, "date")]

df1 = spark.createDataFrame(data1, ["id", "fruit"])
df2 = spark.createDataFrame(data2, ["id", "fruit"])

union_df = df1.union(df2)
union_df.show()
spark.stop()
