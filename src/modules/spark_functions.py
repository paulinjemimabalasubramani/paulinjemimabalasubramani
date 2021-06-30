
"""
Library for starting a Spark session


USEFUL LINKS:
Download JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15

https://github.com/microsoft/sql-spark-connector

https://spark.apache.org/docs/latest/configuration

"""

# %% libraries
from .common_functions import make_logging, catch_error
from .config import Config, is_pc, extraClassPath, config_path

import os

from pyspark.sql import SparkSession


# %% Logging
logger = make_logging(__name__)



# %% Create Spark

@catch_error(logger)
def create_spark():
    """
    Initiate a new spark session
    """
    app_name = os.path.basename(__file__)

    spark = (
        SparkSession
        .builder
        .appName(app_name)
        .config('fs.wasbs.impl', 'org.apache.hadoop.fs.azure.NativeAzureFileSystem')
        )

    if is_pc:
        spark = (spark
        .config('spark.driver.extraClassPath', extraClassPath)
        .config('spark.dynamicAllocation.schedulerBacklogTimeout', '20s')
        .config('spark.executor.memory', '2g')
        .config('spark.python.worker.memory', '1g')
        )

    spark = spark.getOrCreate()
    spark.getActiveSession()

    print(f"\nSpark version = {spark.version}")
    print(f"Hadoop version = {spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}\n")

    return spark



# %% Read SQL Table

@catch_error(logger)
def read_sql(spark, user:str, password:str, schema:str, table:str, database:str, server:str):
    """
    Read a table from SQL server
    """
    print(f"Reading SQL: server='{server}', database='{database}', table='{schema}.{table}'")

    url = f'jdbc:sqlserver://{server};databaseName={database};trustServerCertificate=true;'

    sql_table = (
        spark.read
            .format("jdbc")
            .option("url", url)
            .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
            .option("user", user)
            .option("password", password)
            .option("dbtable", f"{schema}.{table}")
            .option("encrypt", "true")
            .option("hostNameInCertificate", "*.database.windows.net")
            .load()
        )
    
    return sql_table



# %% Read XML File

@catch_error(logger)
def read_xml(spark, file_path:str, rowTag:str="?xml", schema=None):
    """
    Read XML Files using Spark
    """
    xml_table = (spark.read
        .format("com.databricks.spark.xml")
        .option("rowTag", rowTag)
        .option("inferSchema", 'false')
        .option("excludeAttribute", 'false')
        .option("ignoreSurroundingSpaces", 'true')
        .option("mode", "PERMISSIVE")
    )

    if schema:
        xml_table = xml_table.schema(schema=schema)

    return xml_table.load(file_path)


# %% Read CSV File

@catch_error(logger)
def read_csv(spark, file_path:str):
    """
    Read CSV File using Spark
    """
    csv_table = (spark.read
        .format('csv')
        .option('header', 'true')
        .load(file_path)
    )

    return csv_table



# %%




