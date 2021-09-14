# %%
# 
# ####Import Parameters
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import getpass
from datetime import datetime


##Assign Variables
# SQL variables
domain = "ibddomain"
user = "svc_ediprolr"
password = "E0d!pr$L"
# table_name = "information_schema.tables"
table_name = 'Branch'
column_name = "information_schema.columns"
numOfFiles = "20"
format = "delta"
TableLocation = "/usr/local/spark/resource/" + table_name
schema = "OLTP"

# Azure variables
storage_account_name = "agfsclakescd"
containername = "ingress"
pathname = "data/financial_professional/LR/"
foldername = "OLTP/"
filename = table_name
blobpath = pathname + foldername

data_path = f"abfs://{containername}@{storage_account_name}.dfs.core.windows.net/{blobpath}/{filename}"

##Set Spark Context
conf = SparkConf()
#conf.set('spark.jars', '/usr/local/spar/resources/jars/spark-mssql-connector_2.11-1.0.0.jar')
spark = SparkSession.builder.appName("read-table").config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").config("fs.azure.account.key.agwfslakescd.dfs.core.windows.net","bX2jea141iPYwxvsArfWIm0JNP3Qv3rOYU7oFjW0h6TFCxxS+J6ekKP+5ATlaaOTdUAed98eGnhpwSJQtRcpUw==").getOrCreate()


def to_string(df, col_types=['timestamp']):
        """
        Convert timestamp's or other types to string - as it cause errors otherwise.
        """
        for col_name, col_type in df.dtypes:
                if not col_types or col_type in col_types:
                        print(f"Converting {col_name} from '{col_type}' to 'string' type")
                        df = df.withColumn(col_name, col(col_name).cast('string'))
        return df

def remove_spaces(df):
        ##Removes spaces from column names##
        new_df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
        return new_df

##Pull Table from SQL Server
table_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", "jdbc:sqlserver://TSQLOLTP01:1433;databaseName=LR;trustServerCertificate=true") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", f'{schema}.{table_name}') \
        .option("user",user) \
        .option("password",password) \
        .option("encrypt", "true") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .load()

table_df = table_df.withColumn('EXECUTION_DATE', lit(str(datetime.now())))
table_df = to_string(table_df, col_types = ['timestamp'])
table_df = remove_spaces(table_df)

table_df.write.format("delta").mode("overwrite").partitionBy('EXECUTION_DATE').save(data_path)
