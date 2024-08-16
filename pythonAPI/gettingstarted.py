from pyspark.sql import SparkSession, Row, Column
from pyspark.sql.functions import upper, pandas_udf, expr
# from pyspark.sql.types import StructType
from datetime import date, datetime
import pandas as pd

spark = SparkSession.builder.appName('gettingstarted').getOrCreate()
df = spark.createDataFrame([
    Row(a=1,b=2.,c='string1',d=date(2024,8,16), e=datetime(2024,8,16,0)),
    Row(a=2,b=3.,c='string2',d=date(2024,8,17), e=datetime(2024,8,17,1)),
    Row(a=3,b=4.,c='string3',d=date(2024,8,18), e=datetime(2024,8,17,2))
])
df.show()
df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
df.show()

pandas_df = pd.DataFrame({
    'a': [1,2,3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
})
df = spark.createDataFrame(pandas_df)
df.printSchema()
df.show(1)
# spark.conf('spark.sql.repl.eagerEval.enabled', True)
df.show(1,vertical=True)
print(df.columns)
df.select('a','b','c').describe().show()
df.collect()
df.take(1)
print(df.toPandas())
print(df.a)
# type(df.c) == type(upper(df.c)) == type(df.c.isNULL())
df.select(df.c).show()
df.withColumn('upper_c',upper(df.c)).show()
df.filter(df.a==1).show()
@pandas_df('long')
def pandas_plus_one(series:pd.Series) -> pd.Series:
    return series + 1
df.select(pandas_plus_one(df.a)).show()
def pandas_filter_func(iterator):
    for pandas_df in iterator:
        yield pandas_df[pandas_df.a == 1]
df.mapInPandas(pandas_filter_func, schema=df.schema).show()

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()
df.groupyb('color').avg().show()

def plus_mean(pandas_df):
    return pandas_df.assign(v1 = pandas_df.v1-pandas_df.v1.mean())
df.groupy('color').applyInPandas(plus_mean, schema=df.schema).show()


df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ('time', 'id', 'v1'))

df2 = spark.createDataFrame(
    [(20000101, 1, 'x'), (20000101, 2, 'y')],
    ('time', 'id', 'v2'))

def merged_df(l, r):
    return pd.merge_ordered(l, r)
df1.groupby('id').cogroup(df2.groupby('id')).applyInPandas(merged_df, schema='time int, id int, v1 double, v2 string')
df.write.orc('x.orc')
df.write.parquet('y.parquet')
df.write.csv('z.csv')

df.createTempView('tableA')
spark.sql('select count(*) from tableA').show()

@pandas_df('integer')
def puls_one(s: pd.Series) -> pd.Series:
    return s+1
spark.udf.register('puls_one', puls_one)
spark.sql('select puls_one(a) from tableA').show()

df.selectExpr('puls_one(a)').show()
df.select(expr('count(*)')>0).show()
spark.stop()
