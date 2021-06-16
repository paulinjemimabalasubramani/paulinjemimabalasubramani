####Import Parameters
from pyspark.sql.functions import lit, col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import shutil
import subprocess
import getpass
from os import listdir


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
spark = SparkSession.builder.config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").config("fs.azure.account.key.agfsclakescd.blob.core.windows.net","SGILPYErZL2RTSGN8/8fHjBLLlwS6ODMyRUIfts8F0p8UYqxcHxz97ujV9ym4RRCXPDUEoViRcCM8AxpLgsrbA==").appName("get-table").getOrCreate()
spark.conf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
spark.conf.set(
	"fs.azure.account.key.agfsclakescd.blob.core.windows.net",
	"SGILPYErZL2RTSGN8/8fHjBLLlwS6ODMyRUIfts8F0p8UYqxcHxz97ujV9ym4RRCXPDUEoViRcCM8AxpLgsrbA=="
)


azfile = spark.read.csv("wasbs://ingress@agfsclakescd.blob.core.windows.net/test.csv")
azfile.show()
