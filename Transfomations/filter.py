from pyspark import SparkContext

sc = SparkContext("local", "FilterExample")
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])


def is_even(x):
    return x % 2 == 0


even_rdd = rdd.filter(is_even)
print(even_rdd.collect())  # Returns [2, 4, 6, 8, 10]


# --in RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FilterExample").getOrCreate()
df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "cherry"), (4, "date")], ["id", "fruit"])

filtered_df = df.filter(col("id") % 2 == 0)
filtered_df.show()
spark.stop()
