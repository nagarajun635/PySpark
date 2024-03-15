# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "distinct_transformation")
#
# # Creating RDD
# rdd = sc.parallelize([1, 2, 3, 4, 5, 1, 2, 3])
#
# # Applying distinct transformation to remove duplicate elements
# distinct_rdd = rdd.distinct()
#
# # Collecting results to driver
# result_rdd = distinct_rdd.collect()
#
# print(result_rdd)


from pyspark.sql import SparkSession, Row

# Creating SparkSession
spark = SparkSession.builder \
    .appName("distinct_transformation") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (1, 'Alice'), (4, 'David')]
df = spark.createDataFrame(data, ['id', 'name'])

# Applying distinct transformation to remove duplicate rows
distinct_df = df.distinct()

# Showing the results
distinct_df.show()
