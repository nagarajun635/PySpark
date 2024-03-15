# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "repartition_transformation")
#
# # Creating RDD with 100 elements and 4 partitions
# rdd = sc.parallelize(range(100), numSlices=4)
#
# # Check the number of partitions before repartition
# print("Number of partitions before repartition:", rdd.getNumPartitions())
#
# # Repartition RDD to 8 partitions
# repartitioned_rdd = rdd.repartition(8)
#
# # Check the number of partitions after repartition
# print("Number of partitions after repartition:", repartitioned_rdd.getNumPartitions())


from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("repartition_transformation") \
    .getOrCreate()

# Creating DataFrame with 100 rows and 4 partitions
data = [(i,) for i in range(100)]
df = spark.createDataFrame(data, ["value"])

# Check the number of partitions before repartition
print("Number of partitions before repartition:", df.rdd.getNumPartitions())

# Repartition DataFrame to 8 partitions
repartitioned_df = df.repartition(8)

# Check the number of partitions after repartition
print("Number of partitions after repartition:", repartitioned_df.rdd.getNumPartitions())
