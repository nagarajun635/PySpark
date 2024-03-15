# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "aggregateByKey_transformation")
#
# # Creating RDD
# rdd = sc.parallelize([(1, 'Alice'), (2, 'Bob'), (1, 'Charlie'), (2, 'David')])
#
# # Applying aggregateByKey transformation to concatenate names for each key
# # Here, we initialize an empty string as the initial value and concatenate names with comma
# # The second argument to aggregateByKey is a tuple of (initial_value, combine_function, merge_function)
# # The combine function is applied on values of each partition and the merge function is applied to merge results from different partitions
# aggregated_rdd = rdd.aggregateByKey(
#     '',  # Initial value
#     lambda aggregate, name: aggregate + ',' + name,  # Combine function
#     lambda aggregate1, aggregate2: aggregate1 + aggregate2  # Merge function
# )
#
# # Collecting results to driver
# result_rdd = aggregated_rdd.collect()
#
# for key, value in result_rdd:
#     print(f"Key: {key}, Concatenated Names: {value[1:]}")  # Skip the leading comma


from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,collect_list

# Creating SparkSession
spark = SparkSession.builder \
    .appName("aggregateByKey_transformation") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (2, 'Bob'), (1, 'Charlie'), (2, 'David')]
df = spark.createDataFrame(data, ['id', 'name'])

# Applying aggregateByKey transformation to concatenate names for each key
aggregated_df = df.groupBy('id').agg(concat_ws(',', collect_list('name')).alias('concatenated_names'))

# Showing the results
aggregated_df.show(truncate=False)
