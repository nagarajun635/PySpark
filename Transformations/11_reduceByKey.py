# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "reduceByKey_transformation")
#
# # Creating RDD
# rdd = sc.parallelize([(1, 'Alice'), (2, 'Bob'), (1, 'Charlie'), (2, 'David')])
#
# # Applying reduceByKey transformation to concatenate names for each key
# reduced_rdd = rdd.reduceByKey(lambda x, y: x + ',' + y)
#
# # Collecting results to driver
# result_rdd = reduced_rdd.collect()
#
# for key, value in result_rdd:
#     print(f"Key: {key}, Concatenated Names: {value}")


from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,collect_list

# Creating SparkSession
spark = SparkSession.builder \
    .appName("reduceByKey_transformation") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (2, 'Bob'), (1, 'Charlie'), (2, 'David')]
df = spark.createDataFrame(data, ['id', 'name'])

# Applying reduceByKey transformation to concatenate names for each key
reduced_df = df.groupBy('id').agg(concat_ws(',', collect_list('name')).alias('concatenated_names'))

# Showing the results
reduced_df.show(truncate=False)
