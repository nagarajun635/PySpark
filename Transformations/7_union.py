# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "union_transformation")
#
# # Creating RDDs
# rdd1 = sc.parallelize([1, 2, 3])
# rdd2 = sc.parallelize([4, 5, 6])
#
# # Applying union transformation to combine RDDs
# union_rdd = rdd1.union(rdd2)
#
# # Collecting results to driver
# result_rdd = union_rdd.collect()
#
# print(result_rdd)

from pyspark.sql import SparkSession, Row

# Creating SparkSession
spark = SparkSession.builder \
    .appName("union_transformation") \
    .getOrCreate()

# Creating DataFrames
data1 = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
data2 = [(4, 'David'), (5, 'Eve'), (6, 'Frank')]
df1 = spark.createDataFrame(data1, ['id', 'name'])
df2 = spark.createDataFrame(data2, ['id', 'name'])

# Applying union transformation to combine DataFrames
union_df = df1.union(df2)

# Showing the results
union_df.show()
