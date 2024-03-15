# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "join_transformation")
#
# # Creating RDDs
# rdd1 = sc.parallelize([(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')])
# rdd2 = sc.parallelize([(1, 25), (2, 30), (4, 35)])
#
# # Applying join transformation to join RDDs by key
# joined_rdd = rdd1.join(rdd2)
#
# # Collecting results to driver
# result_rdd = joined_rdd.collect()
#
# for key, (value1, value2) in result_rdd:
#     print(f"Key: {key}, Values: ({value1}, {value2})")


from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("join_transformation") \
    .getOrCreate()

# Creating DataFrames
data1 = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
data2 = [(1, 25), (2, 30), (4, 35)]
df1 = spark.createDataFrame(data1, ['id', 'name'])
df2 = spark.createDataFrame(data2, ['id', 'age'])

# Applying join transformation to join DataFrames by key
joined_df = df1.join(df2, 'id', 'inner')

# Showing the results
joined_df.show()
