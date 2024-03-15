# from pyspark import SparkContext
#
# # Creating SparkContext
# sc = SparkContext("local", "flatMap_example")
#
# # Creating RDD
# rdd = sc.parallelize([1, 2, 3])
#
# # Applying flatMap transformation to generate multiple values for each element
# mapped_rdd = rdd.flatMap(lambda x: [x, x * 2, x * 3])
#
# # Collecting results to driver
# result_rdd = mapped_rdd.collect()
#
# print(result_rdd)


from pyspark.sql import SparkSession, Row

# Creating SparkSession
spark = SparkSession.builder \
    .appName("flatMap_example") \
    .getOrCreate()

# Creating DataFrame
data = [(1, 'Alice,Bob'), (2, 'Charlie'), (3, 'David,Eve')]
df = spark.createDataFrame(data, ['id', 'names'])

# Defining flatMap function to split names and generate multiple rows
def flat_map_names(row):
    names = row.names.split(',')
    return [Row(id=row.id, name=name) for name in names]

# Applying flatMap transformation using RDD conversion and back to DataFrame
mapped_df = df.rdd.flatMap(flat_map_names).toDF()

# Showing the results
mapped_df.show()
