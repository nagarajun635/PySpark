from pyspark import SparkContext

sc = SparkContext("local", "MapPartitionsExample")
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)  # Creating an RDD with 3 partitions


def process_partition(iterable):
    return [x * 2 for x in iterable]


processed_rdd = rdd.mapPartitions(process_partition)
print(processed_rdd.glom().collect())

#--in DF
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("MapPartitionsExample").getOrCreate()
df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "cherry"), (4, "date")], ["id", "fruit"])

# Define a UDF (User Defined Function) that processes a partition
def process_partition(iterable):
    result = []
    for row in iterable:
        result.append((row.id * 2, row.fruit.upper()))
    return result

processed_df = df.select(expr("id"), expr("fruit")).rdd.mapPartitions(process_partition)
processed_df = processed_df.toDF(["doubled_id", "uppercase_fruit"])
processed_df.show()
spark.stop()