from pyspark import SparkContext

sc = SparkContext("local", "IntersectionExample")
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4, 5, 6])

intersection_rdd = rdd1.intersection(rdd2)
print(intersection_rdd.collect())  # Returns [3, 4]


#in DF

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IntersectionExample").getOrCreate()
data1 = [(1, "apple"), (2, "banana"), (3, "cherry")]
data2 = [(3, "cherry"), (4, "date")]

df1 = spark.createDataFrame(data1, ["id", "fruit"])
df2 = spark.createDataFrame(data2, ["id", "fruit"])

intersection_df = df1.intersect(df2)
intersection_df.show()
spark.stop()