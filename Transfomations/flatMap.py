from pyspark import SparkContext


sc = SparkContext("local", "FlatMapExample")
sc.setLogLevel("ALL")
rdd = sc.parallelize([1, 2, 3, 4])


def duplicate_and_square(x):
    return (x, x ** 2)

flat_mapped_rdd = rdd.flatMap(duplicate_and_square)
print(flat_mapped_rdd.collect())
# print(sum(flat_mapped_rdd.collect()))
sc.stop()
#in DF

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import expr, explode
#
# spark = SparkSession.builder.appName("FlatMapExample").getOrCreate()
# df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "cherry")], ["id", "fruit"])
#
# flattened_df = df.select(expr("id"), expr("explode(split(upper(fruit), '')) as letter"))
# flattened_df.show()
# spark.stop()
