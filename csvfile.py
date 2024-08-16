from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('csv').getOrCreate()

sc = spark.sparkContext

csvDF1 = spark.read.csv('file:///home/bigdata/spark/examples/src/main/resources/people.csv')
csvDF1.show()

csvDF2 = spark.read.option('delimiter',';').csv('file:///home/bigdata/spark/examples/src/main/resources/people.csv')
csvDF2.show()

csvDF3 = spark.read.option('header',True).option('delimiter',';').csv('file:///home/bigdata/spark/examples/src/main/resources/people.csv')
csvDF3.show()

csvDF4 = spark.read.options(header='true',delimiter=';').csv('file:///home/bigdata/spark/examples/src/main/resources/people.csv')
csvDF4.show()
spark.stop()
sc.stop()