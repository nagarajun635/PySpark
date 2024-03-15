# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "sample_transformation")
#
# # Creating RDD
# rdd = sc.parallelize(range(1, 101))
#
# # Applying sample transformation to take a sample of 10% of the data with replacement and a specific seed
# sampled_rdd = rdd.sample(withReplacement=True, fraction=0.1, seed=42)
#
# # Collecting results to driver
# result_rdd = sampled_rdd.collect()
#
# print(result_rdd)


from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("sample_transformation") \
    .getOrCreate()

# Creating DataFrame
data = [(i, f"Name_{i}") for i in range(1, 101)]
df = spark.createDataFrame(data, ['id', 'name'])

# Applying sample transformation to take a sample of 20% of the data without replacement and a specific seed
sampled_df = df.sample(withReplacement=False, fraction=0.2, seed=42)

# Showing the results
sampled_df.show()
