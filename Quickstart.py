from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions as sf


conf = SparkConf()
sc = SparkContext(conf=conf)
# sc.setLogLevel('ALL')
spark = SparkSession.builder.appName('Quickstart').getOrCreate()

textFile = spark.read.text('textFile')
print(textFile.count())
print(textFile.first())
lineWithLorem = textFile.filter(textFile.value.contains('Lorem'))
print(lineWithLorem.collect())
print(lineWithLorem.count())
maxWordCounts = textFile.select(sf.size(sf.split(textFile.value, '\s+')).name('numWords'))\
    .agg(sf.max(sf.col('numWords')))
print(maxWordCounts.collect())
wordCounts = textFile.select(sf.explode(sf.split(textFile.value, '\s+')).alias('words')).groupby('words').count()
print(wordCounts.collect())
print(type(wordCounts))
filtered_rdd = wordCounts.filter(lambda x: x[1]>1)
print(filtered_rdd.show())
wordCounts.createTempView('text')
spark.sql('select * from text where count>2 order by count').show()
spark.stop()
sc.stop()
