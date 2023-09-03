from pyspark import SparkContext

sc = SparkContext("local", "GroupByKeyExample")
rdd = sc.parallelize([(1, "apple"), (2, "banana"), (1, "cherry"), (2, "date")])

grouped_rdd = rdd.groupByKey()
print(grouped_rdd.collect())

#in DF
