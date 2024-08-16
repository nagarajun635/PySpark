from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('json').getOrCreate()

sc = spark.sparkContext

jdf = spark.read.json('file:///home/bigdata/spark/examples/src/main/resources/people.json')
jdf.show()
jdf.printSchema()

jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
otherPeopleRDD = sc.parallelize(jsonStrings)
jf = spark.read.json(otherPeopleRDD)
jf.show()


spark.sql('set spark.sql.session.timeZone').show()
spark.stop()
sc.stop()
