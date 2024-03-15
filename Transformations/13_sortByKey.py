# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "sortByKey_transformation")
#
# # Creating RDD
# rdd = sc.parallelize([(3, 'Alice'), (1, 'Bob'), (2, 'Charlie'), (4, 'David')])
#
# # Applying sortByKey transformation to sort RDD by key
# sorted_rdd = rdd.sortByKey()
#
# # Collecting results to driver
# result_rdd = sorted_rdd.collect()
#
# for key, value in result_rdd:
#     print(f"Key: {key}, Value: {value}")


from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("sortByKey_transformation") \
    .getOrCreate()

# Creating DataFrame
data = [(3, 'Alice'), (1, 'Bob'), (2, 'Charlie'), (4, 'David')]
df = spark.createDataFrame(data, ['id', 'name'])

# Applying sortBy transformation to sort DataFrame by key
sorted_df = df.orderBy('id')

# Showing the results
sorted_df.show()
