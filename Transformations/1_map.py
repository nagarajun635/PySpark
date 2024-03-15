from pyspark import SparkContext

# Creating SparkContext
sc = SparkContext("local", "map_example")

# Creating RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Applying map transformation to double each element
mapped_rdd = rdd.map(lambda x: x * 2)

# Collecting results to driver
result_rdd = mapped_rdd.collect()

print(result_rdd)




from pyspark.sql import SparkSession, Row

# Creating SparkSession
spark = SparkSession.builder \
    .appName("map_example") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve')]
df = spark.createDataFrame(data, ['id', 'name'])

# Defining map function to concatenate a string with each name
def map_name_with_prefix(row):
    return Row(id=row.id, prefixed_name='Prefix_' + row.name)

# Applying map transformation using RDD conversion and back to DataFrame
mapped_rdd = df.rdd.map(map_name_with_prefix)
mapped_df = spark.createDataFrame(mapped_rdd)

# Showing the results
mapped_df.show()
