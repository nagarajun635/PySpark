# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "filter_example")
#
# # Creating RDD
# rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
#
# # Applying filter transformation to keep only even numbers
# filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
#
# # Collecting results to driver
# result_rdd = filtered_rdd.collect()
#
# print(result_rdd)


from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("filter_example") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve')]
df = spark.createDataFrame(data, ['id', 'name'])

# Applying filter transformation to keep names starting with 'A'
filtered_df = df.filter(df['name'].startswith('A'))

# Showing the results
filtered_df.show()
