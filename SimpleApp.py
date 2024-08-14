from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split, max, col, explode
from pprint import pprint


spark = SparkSession.builder.appName('SimpleApp').getOrCreate()
textFile = spark.read.text('file:///home/bigdata/PycharmProjects/PySpark/files/README.md').cache()
a = textFile.filter(textFile.value.contains('a')).count()
b = textFile.filter(textFile.value.contains('b')).count()
print(f'Number of lines with a {a},Number of lines with a {b}')
spark.stop()
