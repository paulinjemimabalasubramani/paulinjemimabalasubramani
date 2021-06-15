
####Import Parameters

from pyspark.sql.functions import lit, col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import shutil
import subprocess
import getpass
from os import listdir
from azure.storage.blob import BlobClient
from io import BytesIO
from pyspark.sql import functions as F




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
spark = SparkSession.builder.config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").config("fs.azure.account.key.agaggrlakescd.dfs.core.windows.net","22SMBMDagReIFofaEClqhyHzK/lT7GZLlpSc2COP3DZqwXzfJ56KCWXWFcBWUg4vpyt/R6No2o/2jZBn7o5xcQ==").appName("get-table").getOrCreate()


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
table_columns_list = table_columns.select("TABLE_NAME").distinct().rdd.flatMap(lambda x: x).collect()
print(table_columns_list)
#Shows each table schema individually
for table in tablelist_list:
    get_table = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", "jdbc:sqlserver://TSQLOLTP01:1433;databaseName=LR;trustServerCertificate=true") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", "OLTP." + table) \
        .option("user",user) \
        .option("password",password) \
        .option("encrypt", "true") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .load()
    get_metadata = column_df.filter((col('TABLE_NAME') == table )  & (col('TABLE_SCHEMA') == schema))

####Azure Info for Storage
    accountname = "agaggrlakescd"
    accountkey = "22SMBMDagReIFofaEClqhyHzK/lT7GZLlpSc2COP3DZqwXzfJ56KCWXWFcBWUg4vpyt/R6No2o/2jZBn7o5xcQ=="
    containername = "ingress"
    pathname = "data/financial_professional/LR/"
    metapath = "metadata/financial_professional/LR/"
    foldername = "OLTP/"
    filename = table
    datablobpath = pathname + foldername
    metablobpath = metapath + foldername
#    block_blob_service = BlobClient(account_name=accountname, account_key=accountkey, account_url="https://" + accountname + ".blob.core.windows.net", container_name=containername, blob_name=blobname)
#    block_blob_service.upload_blob(get_table)
    data_path = f"abfs://{containername}@{accountname}.dfs.core.windows.net/{datablobpath}/{filename}"
    meta_path = f"abfs://{containername}@{accountname}.dfs.core.windows.net/{metablobpath}/{filename}"
##replace timestamp with string
    get_table = to_string(get_table, col_types = ['timestamp'])
    get_table = remove_spaces(get_table)
###writes parquet
#    get_table.write.parquet(path = data_path, mode='overwrite')
###writes delta
    get_table.write.format("delta").mode("overwrite").save(data_path)
    get_metadata.write.format("delta").mode("overwrite").save(meta_path)
#########Shows Table
    get_table.show()
    get_metadata.show()
