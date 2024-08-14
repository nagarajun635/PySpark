from pyspark import SparkConf, SparkContext
from my_module import maps, reds


conf = SparkConf().setAppName('RDDs').setMaster('spark://debian:7077')
sc = SparkContext(conf=conf)

# distData = sc.parallelize([i for i in range(6)],5)
# print('These are the numer of partitions',distData.getNumPartitions())
# print(distData.reduce(lambda a, b: a + b))
# distFile = sc.textFile('file:///home/bigdata/PycharmProjects/PySpark/files/distFile.txt')
# print('this is sum',distFile.map(lambda s: len(s)).reduce(lambda a, b: a+b))

# rdd = sc.parallelize(range(10)).map(lambda x: (x, x*'x'))
# rdd.saveAsSequenceFile('file:///home/bigdata/PycharmProjects/PySpark/files/rdd.seq')
# rdd = sc.sequenceFile('file:///home/bigdata/PycharmProjects/PySpark/files/rdd.seq')
# print(rdd.collect())


# # passing functions way 1:
# distData = sc.parallelize([i for i in range(11)])
# print(distData.collect())
# print(distData.values())
#
#
# def reds(a, b):
#     return a+b
#
#
# def maps(s):
#     return s*s
#
#
# print(distData.map(maps).reduce(reds))

# # passing functions way 2:
# distData = sc.parallelize([i for i in range(11)])
# print(distData.collect())
# print(distData.map(lambda x: x**x).reduce(lambda y, z:y+z))

# # passing functions way 3:
# distData = sc.parallelize([i for i in range(11)])
# print(distData.collect())
# print(distData.map(maps).reduce(reds))

# bcv = sc.broadcast([1,2,3,4,5])
# print(bcv.value)

acc =  sc.accumulator(0)
print(sc.parallelize(range(10)).foreach(lambda x: acc.add(x)))
sc.stop()
