from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('rouh').getOrCreate()

#  intenal data
# distData = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
# sm = distData.reduce(lambda a, b: a+b)
# print(sm)
# print(type(sm))

#  External data
distFile = spark.read.text('textFile')  #.flatMap(lambda line: line.split(' ')).toDF()
# numLines = distFile.map(lambda s: len(s))
# print(numLines)
# numWords = numLines.reduce(lambda a, b: a+b)
# print(numWords)

# lines = distFile.map(lambda x: (x, 1))
# words = lines.reduceByKey(lambda a, b: a+b)
# filtered_df = words.map(lambda x: x if x[1] >= 4 else '0')
# print(filtered_df.collect())

word_df = distFile.select()

spark.stop()
