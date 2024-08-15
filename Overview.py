from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split, max, col, explode
from pprint import pprint

spark = SparkSession.builder.appName('Overview').getOrCreate()
textFile = spark.read.text('file:///home/bigdata/PycharmProjects/PySpark/files/README.md')
#  Printing number of rows in the file
print(textFile.count())
#  Printing First Row
print(textFile.first())
linesWithSpark = textFile.filter(textFile.value.contains('spark'))
print(textFile.filter(textFile.value.contains('spark')).count())
print(textFile.select(size(split(textFile.value, '\s+')).name('numWords')).agg(max(col('numWords'))).collect())
wordCounts = textFile.select(explode(split(textFile.value, '\s+')).alias('words')).groupBy('words').count()
pprint(wordCounts.collect())
pprint(linesWithSpark.collect())
pprint(textFile.select(max(size(split(textFile.value, '\s+'))).alias('splitted')).collect())
spark.stop()
