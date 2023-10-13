from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sparkContext=sc)
df = sc.textFile('/home/spark/ml-100k/u.data').map(lambda x: x.split('\t')).toDF()\
    .withColumnRenamed('_1', 'col_1').withColumnRenamed('_2', 'col_2')\
    .withColumnRenamed('_3', 'col_3').withColumnRenamed('_4', 'col_4')
# df = spark.read.text('/home/spark/ml-100k/u.data').rdd.map(lambda x: x.split('\t')).toDF()
# print(df.schema.json())
# rows = df.collect()
# row = rows[0]
# print(row.__getitem__("col_1"), row.__getitem__("col_2"), row.__getitem__("col_3"), row.__getitem__("col_4"))
# sel_df = df.select("col_1").collect()
# sel_row = sel_df[1]
# print(sel_row.__getitem__("col_1"))
# print('sum_of_col1')
# sum_df = df.select(sum('col_1')).withColumnRenamed('sum(col_1)','sum_of_col1')
# print('type of dataframe')
# print(type(sum_df))

# order_df = df.select('col_1','col_2').orderBy('col_1').where("col_1>=100")#.show()
# print('type of dataframe')
# print(type(order_df))
df.createTempView('people')
spark.sql('select col_3,count(*) from people group by col_3 order by col_3').withColumnRenamed('col_3', 'catagory')\
    .show()
spark.sql('drop view people')
spark.stop()
sc.stop()



# from pyspark.sql import SparkSession, Row
# from datetime import datetime, date
# import pandas as pd
#
#
# spark = SparkSession.builder.appName('creatingDataframe').getOrCreate()
# df = spark.createDataFrame([
#     Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2003, 2, 4, 12, 1, 20)),
#     Row(a=2, b=3., c='string2', d=date(2000, 1, 1), e=datetime(2003, 2, 4, 12, 1, 20)),
#     Row(a=3, b=4., c='string3', d=date(2000, 1, 1), e=datetime(2003, 2, 4, 12, 1, 20)),
# ], schema='a long, b double, c string, d date, e timestamp')
# df.printSchema()
# df.show()
#
# pandas_df = pd.DataFrame({
#     'a': [1, 2, 3],
#     'b': [2., 3., 4.],
#     'c': ['string1', 'string2', 'string3'],
#     'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
#     'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
# })
# print(pandas_df)
#
# df = spark.createDataFrame(pandas_df)
# df.show(1,vertical=True)
# df.select('a','b','c').describe().show()
# # print(df.collect())
# print()
# df.toPandas()
# spark.stop()
