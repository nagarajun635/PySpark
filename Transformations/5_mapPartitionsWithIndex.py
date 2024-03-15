# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "mapPartitionsWithIndex_example")
#
# # Creating RDD
# rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)
#
# # Define a function to double each element in a partition based on index
# def double_partition_with_index(index, iterator):
#     return map(lambda x: (index, x * 2), iterator)
#
# # Applying mapPartitionsWithIndex transformation to double each element in each partition
# mapped_rdd = rdd.mapPartitionsWithIndex(double_partition_with_index)
#
# # Collecting results to driver
# result_rdd = mapped_rdd.collect()
#
# print(result_rdd)


from pyspark.sql import SparkSession, Row

# Creating SparkSession
spark = SparkSession.builder \
    .appName("mapPartitionsWithIndex_example") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve')]
df = spark.createDataFrame(data, ['id', 'name'])

# Define a function to add index to each row in a partition
def add_index_partition(index, iterator):
    return [Row(id=index + row.id, name=row.name) for row in iterator]

# Applying mapPartitionsWithIndex transformation to add index to each row in each partition
mapped_df = df.rdd.mapPartitionsWithIndex(add_index_partition).toDF()

# Showing the results
mapped_df.show()
