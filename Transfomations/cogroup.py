from pyspark import SparkContext

sc = SparkContext("local", "CogroupExample")
rdd1 = sc.parallelize([(1, "apple"), (2, "banana"), (3, "cherry")])
rdd2 = sc.parallelize([(2, "yellow"), (3, "red"), (4, "green")])

cogrouped_rdd = rdd1.cogroup(rdd2)
print(cogrouped_rdd.collect())
