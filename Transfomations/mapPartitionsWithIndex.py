from pyspark import SparkContext

sc = SparkContext("local", "MapPartitionsWithIndexExample")
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)  # Creating an RDD with 3 partitions


def process_partition_with_index(partition_index, iterable):
    return [f"Partition {partition_index}: {x * 2}" for x in iterable]


processed_rdd = rdd.mapPartitionsWithIndex(process_partition_with_index)
print(processed_rdd.collect())

#in DF
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("MapPartitionsWithIndexExample").getOrCreate()
df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "cherry"), (4, "date")], ["id", "fruit"])

# Define a function that processes a partition with index
def process_partition_with_index(partition_index, iterable):
    result = []
    for row in iterable:
        result.append((partition_index, row.id * 2, row.fruit.upper()))
    return result

rdd = df.rdd
processed_rdd = rdd.mapPartitionsWithIndex(process_partition_with_index)
processed_df = processed_rdd.toDF(["partition_index", "doubled_id", "uppercase_fruit"])
processed_df.show()
spark.stop()