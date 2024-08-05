from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg


spark = SparkSession.builder.appName('Examples').getOrCreate()
df = spark.createDataFrame([("sue", 32), ("li", 3), ("bob", 75), ("heo", 13)], schema='name string, age int')
df.printSchema()
df.show()
print(df.columns)
print(df.collect())
df1 = df.withColumn('life_stage', when(col('age') < 13, 'child').
                    when(col('age').between(13, 19), 'teenager').
                    otherwise('adult'))
df1.show()
df.show()
df1.where(col('life_stage').isin(['teenager', 'adult'])).show()
df1.select(avg('age')).show()
df1.groupBy(col('life_stage')).avg().show()
spark.sql('select avg(age) from {df1}', df1=df1).show()
spark.sql('select life_stage,avg(age) from {df1} group by life_stage', df1=df1).show()
df1.write.mode('overwrite').saveAsTable('some_people')
spark.sql('select * from some_people').show()
spark.sql('insert into some_people values ("naga",27,"adult")')
spark.sql('select * from some_people').show()
spark.sql('select * from some_people where life_stage="adult"').show()
spark.stop()
