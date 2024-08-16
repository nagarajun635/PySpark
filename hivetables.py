from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName('hivetables').enableHiveSupport().getOrCreate()

# spark.sql('create table src(key int, value string) using hive')
# spark.sql('load data local inpath "file:///home/bigdata/spark/examples/src/main/resources/kv1.txt" into table src')

spark.sql('select * from src').show()

spark.sql('select count(*) from src').show()

sqlDF = spark.sql('select * from src where key<10 order by key')
sqlDF.show()

stringsDS = sqlDF.rdd.map(lambda row: 'Key: %d, Value: %s'%(row.key, row.value))
for record in stringsDS.collect():
    print(record)
Record = Row('key', 'value')
recordsDF = spark.createDataFrame([Record(i, 'val_'+str(i)) for i in range(101)])
recordsDF.show()
recordsDF.createTempView('resource')
spark.sql('select * from resource r join src s on s.key=r.key').show()
spark.stop()
