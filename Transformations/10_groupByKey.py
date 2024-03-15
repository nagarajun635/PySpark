# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "groupByKey_transformation")
#
# # Creating RDD
# rdd = sc.parallelize([(1, 'Alice'), (2, 'Bob'), (1, 'Charlie'), (2, 'David')])
#
# # Applying groupByKey transformation to group values by key
# grouped_rdd = rdd.groupByKey()
#
# # Collecting results to driver
# result_rdd = grouped_rdd.collect()
#
# for key, values in result_rdd:
#     print(f"Key: {key}, Values: {list(values)}")


from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

# Creating SparkSession
spark = SparkSession.builder \
    .appName("groupByKey_transformation") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (2, 'Bob'), (1, 'Charlie'), (2, 'David')]
df = spark.createDataFrame(data, ['id', 'name'])

# Applying groupBy transformation to group values by key
grouped_df = df.groupBy('id').agg(collect_list('name').alias('names'))

# Showing the results
grouped_df.show(truncate=False)
