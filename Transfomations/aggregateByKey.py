from pyspark import SparkContext

sc = SparkContext("local", "AggregateByKeyExample")
rdd = sc.parallelize([(1, 10), (2, 20), (1, 30), (2, 15)])

def seq_op(acc, value):
    return acc + value

def comb_op(acc1, acc2):
    return acc1 + acc2

initial_value = 0
result_rdd = rdd.aggregateByKey(initial_value, seq_op, comb_op)
print(result_rdd.collect())

#in DF
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,expr

spark = SparkSession.builder.appName("AggregateByKeyExample").getOrCreate()
data = [(1, 10), (2, 20), (1, 30), (2, 15)]

df = spark.createDataFrame(data, ["key", "value"])

# Custom aggregation functions
def seq_op(acc, value):
    return acc + value

def comb_op(acc1, acc2):
    return acc1 + acc2

initial_value = 0

result_df = df.groupby("key").agg(
    sum("value").alias("sum_value"),
    sum("value").alias("initial_sum")
).select("key", expr(f"aggregateByKey(initial_sum, x -> {initial_value}, {seq_op.__name__}, {comb_op.__name__}) as custom_sum"))

result_df.show()
