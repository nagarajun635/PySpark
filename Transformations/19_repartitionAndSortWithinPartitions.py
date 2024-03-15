# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "repartition_sort_rdd")
#
# # Creating RDD with key-value pairs
# data = [(1, 'Alice'), (3, 'Charlie'), (2, 'Bob'), (4, 'David'), (5, 'Eve')]
# rdd = sc.parallelize(data)
#
# # Repartition and sort RDD within partitions based on the key
# repartitioned_sorted_rdd = rdd.repartitionAndSortWithinPartitions(3, lambda x: x)
#
# # Collecting results to driver
# result = repartitioned_sorted_rdd.collect()
#
# # Print the result
# for item in result:
#     print(item)


from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("repartition_sort_df") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (3, 'Charlie'), (2, 'Bob'), (4, 'David'), (5, 'Eve')]
df = spark.createDataFrame(data, ['id', 'name'])

# Repartition DataFrame to 3 partitions based on 'id'
repartitioned_df = df.repartition(3, 'id')

# Sort DataFrame within each partition based on 'id'
sorted_df = repartitioned_df.sortWithinPartitions('id')

# Showing the results
sorted_df.show()
