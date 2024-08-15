from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder.appName('gettingstarted').getOrCreate()
path = 'file:///home/bigdata/spark/examples/src/main/resources/people.json'
df = spark.read.json(path)
df.show()
df.printSchema()
df.select('name').show()
df.select(df['name'], df['age']+1,df['age']).show()
df.filter(df['age']>20).show()
df.groupBy('age').count().show()
df.createOrReplaceTempView('people')
sqlDF = spark.sql('select * from people')
sqlDF.show()
df.createGlobalTempView('people1')
sqlDF1 = spark.sql('select * from global_temp.people1')
sqlDF1.show()
spark.newSession().sql('select * from global_temp.people1').show()

sc = spark.sparkContext
lines = sc.textFile('file:///home/bigdata/spark/examples/src/main/resources/people.txt')
# parts = lines.map(lambda l: l.split(','))
# people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
# print(people.collect())
# schemaPeople = spark.createDataFrame(people)
# schemaPeople.createTempView('peoples')
# teenagers = spark.sql('select * from peoples where age between 13 and 19')
# teenNames = teenagers.rdd.map(lambda n:"Name: "+n.name).collect()
# for name in teenNames:
#     print(name)

parts = lines.map(lambda p: p.split(','))
people = parts.map(lambda p: (p[0],int(p[1].strip())))
fields = [StructField('name', StringType(), True),StructField('age', IntegerType(), True)]
schema = StructType(fields)
schemaPeople = spark.createDataFrame(people, schema)
schemaPeople.createTempView('peple')
spark.sql('select age from peple').show()

spark.stop()