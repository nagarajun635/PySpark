from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('generic_load_save').getOrCreate()
path = 'file:///home/bigdata/spark/examples/src/main/resources/users.parquet'
df1 = spark.read.load(path)
df1.select('name', 'favorite_color').show()

df2 = spark.read.load('file:///home/bigdata/spark/examples/src/main/resources/people.json', format='json')
df2.show()

df3 = spark.read.csv('file:///home/bigdata/spark/examples/src/main/resources/people.csv', sep=';', inferSchema=True, header=True)
df3.show()

df4 = spark.read.orc('file:///home/bigdata/spark/examples/src/main/resources/users.orc')
df4.show()

df5 = spark.read.parquet(path)
df5.show()

df6 = spark.sql('select * from parquet.`file:///home/bigdata/spark/examples/src/main/resources/users.parquet`')
df6.show()
spark.stop()