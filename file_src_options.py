from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('file_src_options').getOrCreate()

test_crpt_df0 = (spark.read.option('ignoreCorruptFiles','true')
                 .parquet("file:///home/bigdata/spark/examples/src/main/resources/dir1/", "file:///home/bigdata/spark/examples/src/main/resources/dir1/dir2/"))
test_crpt_df0.show()

spark.sql('set spark.sql.files.ignoreCorruptFiles=true')
test_crpt_df1 = (spark.read.parquet("file:///home/bigdata/spark/examples/src/main/resources/dir1/", "file:///home/bigdata/spark/examples/src/main/resources/dir1/dir2/"))
test_crpt_df1.show()

# spark.sql.files.ignoreMissingFiles=true

df0 = spark.read.load("file:///home/bigdata/spark/examples/src/main/resources/dir1/", format='parquet', pathGlobFilter='*.parquet')
df0.show()

recursive_loaded_df = spark.read.format('parquet').option('recursiveFileLookup','true').load("file:///home/bigdata/spark/examples/src/main/resources/dir1/")
recursive_loaded_df.show()
print('before')
bdf = spark.read.load("file:///home/bigdata/spark/examples/src/main/resources/dir1/",format='parquet',modifiedBefore='2050-08-01T03:56:00')
bdf.show()
print('after')
adf = spark.read.load("file:///home/bigdata/spark/examples/src/main/resources/dir1/",format='parquet',modifiedAfter='2024-08-01T03:56:00')
adf.show()

spark.stop()