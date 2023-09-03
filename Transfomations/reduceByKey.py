from pyspark import SparkContext

sc = SparkContext("local", "ReduceByKeyExample")
rdd = sc.parallelize([(1, 10), (2, 20), (1, 30), (2, 15)])

def sum_values(a, b):
    return a + b

result_rdd = rdd.reduceByKey(sum_values)
print(result_rdd.collect())

#in DF
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("ReduceByKeyExample").getOrCreate()
data = [(1, 10), (2, 20), (1, 30), (2, 15)]

df = spark.createDataFrame(data, ["key", "value"])
result_df = df.groupBy("key").agg(sum("value").alias("sum_value"))
result_df.show()
spark.stop()
