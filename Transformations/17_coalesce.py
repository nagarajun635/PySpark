# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "coalesce_transformation")
#
# # Creating RDD with 8 partitions
# rdd = sc.parallelize(range(100), numSlices=8)
#
# # Check the number of partitions before coalesce
# print("Number of partitions before coalesce:", rdd.getNumPartitions())
#
# # Coalesce RDD to 4 partitions
# coalesced_rdd = rdd.coalesce(4)
#
# # Check the number of partitions after coalesce
# print("Number of partitions after coalesce:", coalesced_rdd.getNumPartitions())



from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("coalesce_transformation") \
    .getOrCreate()

# Creating DataFrame with 8 partitions
data = [(i,) for i in range(100)]
df = spark.createDataFrame(data, ["value"])

# Check the number of partitions before coalesce
print("Number of partitions before coalesce:", df.rdd.getNumPartitions())

# Coalesce DataFrame to 4 partitions
coalesced_df = df.coalesce(4)

# Check the number of partitions after coalesce
print("Number of partitions after coalesce:", coalesced_df.rdd.getNumPartitions())
