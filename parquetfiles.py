from pyspark.sql import Row, SparkSession


spark = SparkSession.builder.appName('parquet').getOrCreate()

sc = spark.sparkContext

# peopleDF = spark.read.json('file:///home/bigdata/spark/examples/src/main/resources/people.json')
#
# peopleDF.write.save('file:///home/bigdata/PycharmProjects/PySpark/files/people.parquet')
#
# parquetDF = spark.read.parquet('file:///home/bigdata/PycharmProjects/PySpark/files/people.parquet')
# parquetDF.createTempView('people')
# spark.sql('select * from people where age between 13 and 19').show()

squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6)).map(lambda x: Row(a=x, b=x**x)))
squaresDF.write.mode('overwrite').parquet('file:///home/bigdata/PycharmProjects/PySpark/files/test_table/key=1')

cubesDF = spark.createDataFrame(sc.parallelize(range(1, 6)).map(lambda y: Row(c=y, d=y**3)))
cubesDF.write.mode('overwrite').parquet('file:///home/bigdata/PycharmProjects/PySpark/files/test_table/key=2')

mergeDF = (spark.read.option('mergeSchema', 'true')
           .load('file:///home/bigdata/PycharmProjects/PySpark/files/test_table/'))
print(mergeDF.collect())
mergeDF.printSchema()
#  to refresh table
spark.catalog.refreshTable('people')
spark.stop()
sc.stop()
