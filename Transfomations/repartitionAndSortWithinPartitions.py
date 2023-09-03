from pyspark import SparkContext

sc = SparkContext("local", "RepartitionAndSortExample")
rdd = sc.parallelize([(1, "apple"), (3, "cherry"), (2, "banana"), (4, "date"), (5, "grape")], 2)
print(rdd.collect())
# Repartition into 3 partitions and sort by the first element of each tuple
sorted_rdd = rdd.repartitionAndSortWithinPartitions(3,lambda x: x%2,True)
print(sorted_rdd.collect())
sc.stop()

#in  DF
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.appName("RepartitionAndSortExample").getOrCreate()
data = [(1, "apple"), (3, "cherry"), (2, "banana"), (4, "date"), (5, "grape")]
columns = ["id", "fruit"]

df = spark.createDataFrame(data, columns)

print(df.rdd.getNumPartitions())
# Repartition into 3 partitions and sort by the "id" column
repartitioned_sorted_df = df.repartition(3, "id").sortWithinPartitions("id")
repartitioned_sorted_df.show()
spark.stop()
