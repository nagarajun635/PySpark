from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim
import findspark

findspark.init()
spark = SparkSession.builder.appName('CSV_Practice').getOrCreate()
path = 'file:///home/bigdata/PycharmProjects/PySpark/files/feed1.csv'
# df = spark.read.csv(path)
df = spark.read.options(delimiter='|',header=True).csv(path)
dfx = df.select([trim(col(column)).alias(column.strip()) for column in df.columns])
ls = df.select([trim(col(column)).alias(column.strip()) for column in df.columns])
print(ls)
dfx.show()
df2 = spark.read.option('delimiter','|').option('header','True').csv(path).show()
dfx.createOrReplaceTempView('dept')
df3 = spark.sql('select name,regexp_substr(dept, "[a-zA-Z]+") AS dept, salary from dept')
df4 = df3.select(col('name'),col('dept'),col('salary').cast('int').alias('salary'))
df4.show()
ln = [trim(col(column)).alias(column.upper().strip()) for column in df4.columns]
print(ln)
df4.printSchema()
spark.stop()