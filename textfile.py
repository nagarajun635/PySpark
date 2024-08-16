from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('csv').getOrCreate()

sc = spark.sparkContext

tdf1 = spark.read.text('file:///home/bigdata/spark/examples/src/main/resources/people.txt')
tdf1.show()

tdf2 = spark.read.text('file:///home/bigdata/spark/examples/src/main/resources/people.txt',lineSep=',')
tdf2.show()

tdf3 = spark.read.text('file:///home/bigdata/spark/examples/src/main/resources/people.txt',wholetext=True)
tdf3.show()

spark.stop()
sc.stop()