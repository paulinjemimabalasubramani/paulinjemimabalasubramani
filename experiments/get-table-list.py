
####Import Parameters

from pyspark.sql.functions import lit, col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import shutil
import subprocess
import getpass
import os
from datetime import datetime
from os import listdir
from azure.storage.blob import BlobClient
from io import BytesIO
from pyspark.sql import functions as F
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient





def connect_to_storage(storage_name):
        ##grabbing env variables
        azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
        #print(azure_tenant_id)
        azure_client_id = os.environ.get("AZURE_KV_ID")
        #print(azure_client_id)
        azure_client_secret = os.environ.get("AZURE_KV_SECRET")
        vault_endpoint = "https://ag-kv-west2-secondary.vault.azure.net/"
        #set azure credentials
        credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
        #set keyvault credentials
        client = SecretClient(vault_endpoint, credential)
        sp_id = client.get_secret(f"qa-{storage_name}-id")
        sp_pass = client.get_secret(f"qa-{storage_name}-pass")
        print(sp_id.value)
        spark.conf.set(f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net",sp_id.value )
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net",sp_pass.value)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{azure_tenant_id}/oauth/token")
        spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")


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
spark = SparkSession.builder.config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").appName("get-table").getOrCreate()


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
    storage_name = accountname
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
    get_table = get_table.withColumn('EXECUTION_DATE', lit(str(datetime.now())))
###writes parquet
#    get_table.write.parquet(path = data_path, mode='overwrite')
###writes delta
###not getting past?
#    connect_to_storage(accountname)


##grabbing env variables
    azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
    #print(azure_tenant_id)
    azure_client_id = os.environ.get("AZURE_KV_ID")
    #print(azure_client_id)
    azure_client_secret = os.environ.get("AZURE_KV_SECRET")
    vault_endpoint = "https://ag-kv-west2-secondary.vault.azure.net/"
    #set azure credentials
    credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
    #set keyvault credentials
    client = SecretClient(vault_endpoint, credential)
    sp_id = client.get_secret(f"qa-{storage_name}-id")
    sp_pass = client.get_secret(f"qa-{storage_name}-pass")
    print(sp_id.value)
    spark.conf.set(f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net",sp_id.value )
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net",sp_pass.value)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token")
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")




    get_table.write.format("delta").mode("overwrite").save(data_path)
    get_metadata.write.format("delta").mode("overwrite").save(meta_path)
#########Shows Table
    get_table.show()
    get_metadata.show()
