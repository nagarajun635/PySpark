from pyspark import SparkContext

sc = SparkContext("local", "MapExample")
sc.setLogLevel('ALL')
rdd = sc.parallelize([1, 2, 3, 4])


def square(x):
    return (x, x ** 2)


squared_rdd = rdd.map(square)
squared_rdd.collect()
print(squared_rdd.collect())

sc.stop()
# In RDD
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import expr
#
# spark = SparkSession.builder.appName("MapExample").getOrCreate()
# df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "cherry")], ["id", "fruit"])
#
# mapped_df = df.select(expr("id * 2 as doubled_id"), expr("upper(fruit) as uppercase_fruit"))
# mapped_df.show()
# spark.stop()
