from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *



user_name = "jfields"
password = "Pr0t0m0lecule!"
table_name = "OLTP.Appointment"


conf = SparkConf()
#conf.set('spark.jars', '/usr/local/spar/resources/jars/spark-mssql-connector_2.11-1.0.0.jar')
spark = SparkSession.builder.appName("send_file").getOrCreate()

jdbc_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", "jdbc:sqlserver://TSQLOLTP01:1433;databaseName=LR;integratedSecurity=true;") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", table_name) \
#	.option("user", "IBDDOMAIN\jfields") \
#	.option("password", "Pr0t0m0lecule!") \
        .option("encrypt", "true") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .load()

display(jdbc_df)
