from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('hivetables').getOrCreate()

mysqldf1 = spark.read.format('jdbc').option('url','jdbc:mysql://localhost:3306').option('dbtable','metastore_db.TBLS').option('user','hive').option('password','root').load()
mysqldf1.show()

mysqldf2 = spark.read.jdbc('jdbc:mysql://localhost:3306', 'metastore_db.TBLS',properties={'user':'hive','password':'root'})
mysqldf2.show()
spark.stop()