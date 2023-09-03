from pyspark.sql import SparkSession, Row
from datetime import date, datetime


spark = SparkSession.builder.appName('creatingDataframe').getOrCreate()
df1 = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2024, 9, 3)),
    Row(a=1, b=2., c='string1', d=date(2023, 9, 3))
])
df1.show()
df2 = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
df2.show()
df3 = spark.createDataFrame([
    [1,2,3,4],[2,3,4,5],[3,4,5,6]
],schema=['a','b','c','d'])
df3.show()
df4 = spark.createDataFrame([
    (2,3,4,5),(3,4,5,6),(5,6,7,8)
],schema=['a','b','c','d'])
df4.show()
spark.stop()
