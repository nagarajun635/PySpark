from pyspark import SparkContext

sc = SparkContext("local", "SampleExample")
rdd = sc.parallelize(range(1, 101))

sampled_rdd = rdd.sample(withReplacement=False, fraction=0.2, seed=42)
print(sampled_rdd.collect())  # Returns a random sample of approximately 20% of the data

#in DF
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SampleExample").getOrCreate()
df = spark.range(1, 101)

sampled_df = df.sample(withReplacement=False, fraction=0.2, seed=42)
sampled_df.show()  # Shows a random sample of approximately 20% of the data
spark.stop()