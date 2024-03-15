# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "intersection_transformation")
#
# # Creating RDDs
# rdd1 = sc.parallelize([1, 2, 3, 4, 5])
# rdd2 = sc.parallelize([4, 5, 6, 7, 8])
#
# # Applying intersection transformation to find common elements
# intersection_rdd = rdd1.intersection(rdd2)
#
# # Collecting results to driver
# result_rdd = intersection_rdd.collect()
#
# print(result_rdd)


from pyspark.sql import SparkSession, Row

# Creating SparkSession
spark = SparkSession.builder \
    .appName("intersection_transformation") \
    .getOrCreate()

# Creating DataFrames
data1 = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve')]
data2 = [(4, 'David'), (5, 'Eve'), (6, 'Frank'), (7, 'George'), (8, 'Helen')]
df1 = spark.createDataFrame(data1, ['id', 'name'])
df2 = spark.createDataFrame(data2, ['id', 'name'])

# Applying intersection transformation to find common rows
intersection_df = df1.intersect(df2)

# Showing the results
intersection_df.show()
