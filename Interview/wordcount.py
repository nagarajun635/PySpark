from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import explode,split


sc = SparkContext()
rdd1 = sc.textFile('file:///home/bigdata/PycharmProjects/PySpark/files/distFile.txt')
rdd2 = rdd1.flatMap(lambda x: x.split(' ')).map(lambda y: (y,1)).reduceByKey(lambda x, y: x+y)
rdd3 = rdd2.collect()
print(rdd2.collect())
for i in rdd3:
    if i[1]>1:
        print(i)
sc.stop()
spark = SparkSession.builder.appName('wordcount').getOrCreate()
df1 = spark.read.text('file:///home/bigdata/PycharmProjects/PySpark/files/distFile.txt')
df2 = df1.select(explode(split(df1.value, '\s+')).alias('words')).groupby('words').count().collect()
print(df2)
spark.stop()
