from pyspark import SparkContext

sc = SparkContext("local", "JoinExample")
rdd1 = sc.parallelize([(1, "apple"), (2, "banana"), (3, "cherry")])
rdd2 = sc.parallelize([(2, "yellow"), (3, "red"), (4, "green")])

joined_rdd = rdd1.join(rdd2)
print(joined_rdd.collect())

# in DF

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinExample").getOrCreate()
data1 = [(1, "apple"), (2, "banana"), (3, "cherry")]
data2 = [(2, "yellow"), (3, "red"), (4, "green")]

df1 = spark.createDataFrame(data1, ["id", "fruit"])
df2 = spark.createDataFrame(data2, ["id", "color"])

joined_df = df1.join(df2, on="id")
joined_df.show()

