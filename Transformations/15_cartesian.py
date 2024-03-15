# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "cartesian_transformation")
#
# # Creating RDDs
# rdd1 = sc.parallelize(['A', 'B'])
# rdd2 = sc.parallelize([1, 2, 3])
#
# # Applying cartesian transformation to compute Cartesian product of RDDs
# cartesian_rdd = rdd1.cartesian(rdd2)
#
# # Collecting results to driver
# result_rdd = cartesian_rdd.collect()
#
# for value in result_rdd:
#     print(value)


from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("cartesian_transformation") \
    .getOrCreate()

# Creating DataFrames
data1 = [('A',), ('B',)]
data2 = [(1,), (2,), (3,)]
df1 = spark.createDataFrame(data1, ['col1'])
df2 = spark.createDataFrame(data2, ['col2'])

# Applying cartesian transformation to compute Cartesian product of DataFrames
cartesian_df = df1.crossJoin(df2)

# Showing the results
cartesian_df.show()
