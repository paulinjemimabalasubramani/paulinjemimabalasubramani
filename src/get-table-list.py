####Import Parameters
from pyspark.sql.functions import lit, col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import getpass


##Assign Variables
domain = "ibddomain"
user = "svc_ediprolr"
password = "E0d!pr$L"
table_name = "information_schema.tables"
column_name = "information_schema.columns"
numOfFiles = "20"
format = "parquet"
TableLocation = "/usr/local/spark/resource/" + table_name
schema = "OLTP"

##Set Spark Context
conf = SparkConf()
#conf.set('spark.jars', '/usr/local/spar/resources/jars/spark-mssql-connector_2.11-1.0.0.jar')
spark = SparkSession.builder.appName("get-table").getOrCreate()



##Pull Table from SQL Server
table_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", "jdbc:sqlserver://TSQLOLTP01:1433;databaseName=LR;trustServerCertificate=true") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", table_name) \
        .option("user",user) \
        .option("password",password) \
        .option("encrypt", "true") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .load()

##Filter Table for values
tablelist_df = table_df.filter("TABLE_TYPE == 'BASE TABLE' and  TABLE_SCHEMA = 'OLTP'")
##Gets and Prints table count
tablelist_count = tablelist_df.count()
print(str(tablelist_count) + " Tables in OLTP")

##Gets and Lists tables in assinged schema
tablelist_list = tablelist_df.select("TABLE_NAME").rdd.flatMap(lambda x: x).collect()
print(*tablelist_list, sep = ", ") 

###check table schema

column_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", "jdbc:sqlserver://TSQLOLTP01:1433;databaseName=LR;trustServerCertificate=true") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", column_name) \
        .option("user",user) \
        .option("password",password) \
        .option("encrypt", "true") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .load()

###Show table Schema for all tables in Schema "OLTP"
table_columns = column_df.filter((col('TABLE_NAME').isin(tablelist_list)) & (col('TABLE_SCHEMA') == schema))
table_columns.show(table_columns.count(), False)
#Shows each table schema individually
#for table in tablelist_list:
#    table_columns = column_df.filter((col('TABLE_NAME') == table) & (col('TABLE_SCHEMA') == schema))
#    print(table.rjust(40, '-'))
#    print(table_columns.count())
#    table_columns.show(table_columns.count(), False)


