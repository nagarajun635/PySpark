from pyspark import SparkContext

sc = SparkContext("local", "CartesianExample")
rdd1 = sc.parallelize([1, 2])
rdd2 = sc.parallelize(["A", "B"])

cartesian_rdd = rdd1.cartesian(rdd2)
print(cartesian_rdd.collect())

#in DF
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CartesianExample").getOrCreate()
df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "letter"])
df2 = spark.createDataFrame([(3, "X"), (4, "Y")], ["id", "letter"])

cartesian_df = df1.crossJoin(df2)
cartesian_df.show()

