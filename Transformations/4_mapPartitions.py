# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "mapPartitions_example")
#
# # Creating RDD
# rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)
#
# # Define a function to double each element in a partition
# def double_partition(iterator):
#     return map(lambda x: x * 2, iterator)
#
# # Applying mapPartitions transformation to double each element in each partition
# mapped_rdd = rdd.mapPartitions(double_partition)
#
# # Collecting results to driver
# result_rdd = mapped_rdd.collect()
#
# print(result_rdd)


from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

# Creating SparkSession
spark = SparkSession.builder \
    .appName("mapPartitions_example") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve')]
df = spark.createDataFrame(data, ['id', 'name'])

# Define a function to add a prefix to each name in a partition
def add_prefix_partition(iterator):
    return [Row(id=row.id, prefixed_name='Prefix_' + row.name) for row in iterator]

# Applying mapPartitions transformation to add a prefix to each name in each partition
mapped_df = df.rdd.mapPartitions(add_prefix_partition).toDF()

# Showing the results
mapped_df.show()
