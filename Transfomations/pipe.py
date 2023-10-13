from pyspark import SparkContext

sc = SparkContext("local", "PipeExample")
rdd = sc.parallelize(['1', '2', '3', '4', '5'])

shell_script = '/home/spark/PycharmProjects/pythonProject/Transfomations/pipe.sh'
print(rdd.collect())
result_rdd = rdd.pipe(shell_script)
# print(result_rdd.sum())
print(result_rdd.collect())
sc.stop()
