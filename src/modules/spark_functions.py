
"""
Library for creating a Spark session and for other common Spark functions


Download SQL Server JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15

https://github.com/microsoft/sql-spark-connector

https://spark.apache.org/docs/latest/configuration

"""

# %% libraries
import os, platform


from .common_functions import make_logging, catch_error
from .config import is_pc, extraClassPath


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
        .config('spark.executor.memory', '4g')
        .config('spark.python.worker.memory', '2g')
        .config('spark.reducer.maxSizeInFlight', '256m')
        .config('spark.shuffle.file.buffer', '256k')
        .config('spark.shuffle.registration.timeout', '8s')
        .config('spark.eventLog.buffer.kb', '1000k')
        .config('spark.serializer.objectStreamReset', '40')
        .config('spark.streaming.backpressure.enabled', 'true')
        .config('spark.streaming.blockInterval', '1200ms')
        )

    spark = spark.getOrCreate()
    spark.getActiveSession()

    print(f'\n{platform.sys.version}')
    print(f'Python version = {platform.python_version()}')
    print(f"Spark version  = {spark.version}")
    print(f"Hadoop version = {spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}\n")

    return spark



# %% Read SQL Table

@catch_error(logger)
def read_sql(spark, schema:str, table_name:str, database:str, server:str, user:str, password:str):
    """
    Read a table from SQL server
    """
    sql_table_name = f'{schema}.{table_name}'
    print(f"Reading SQL: server='{server}', database='{database}', table='{sql_table_name}'")

    url = f'jdbc:sqlserver://{server};databaseName={database};trustServerCertificate=true;'

    sql_table = (
        spark.read
            .format("jdbc")
            .option("url", url)
            .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
            .option("user", user)
            .option("password", password)
            .option("dbtable", f"{sql_table_name}")
            .option("encrypt", "true")
            .option("hostNameInCertificate", "*.database.windows.net")
            .load()
        )
    
    return sql_table



# %% Write table to SQL server

@catch_error(logger)
def write_sql(table, table_name:str, schema:str, database:str, server:str, user:str, password:str):
    """
    Write table to SQL server
    """
    sql_table_name = f'{schema}.{table_name}'
    print(f"\nWriting to SQL Server: server='{server}', database='{database}', table='{sql_table_name}'")

    url = f'jdbc:sqlserver://{server};databaseName={database};trustServerCertificate=true;'

    table.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver') \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", sql_table_name) \
        .option("encrypt", "true") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .save()

    print('Finished Writing to SQL Server\n')



# %% Function to Get Snowflake Table

@catch_error(logger)
def read_snowflake(spark, table_name:str, schema:str, database:str, warehouse:str, role:str, account:str, user:str, password:str):
    print(f"\nReading Snowflake: role='{role}', warehouse='{warehouse}', database='{database}', table='{schema}.{table_name}'")
    sf_options = {
        'sfUrl': f'{account}.snowflakecomputing.com',
        'sfUser': user,
        'sfPassword': password,
        'sfRole': role,
        'sfWarehouse': warehouse,
        'sfDatabase': database,
        'sfSchema': schema,
        }

    table = spark.read \
        .format('snowflake') \
        .options(**sf_options) \
        .option('dbtable', table_name) \
        .load()

    print('Finished Reading from Snowflake\n')
    return table



# %% Read XML File

@catch_error(logger)
def read_xml(spark, file_path:str, rowTag:str="?xml", schema=None):
    """
    Read XML Files using Spark
    """
    print(f'\nReading XML file: {file_path}')
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

    xml_table_load = xml_table.load(file_path)
    print('Finished reading XML file\n')
    return xml_table_load


# %% Read CSV File

@catch_error(logger)
def read_csv(spark, file_path:str):
    """
    Read CSV File using Spark
    """
    print(f'\nReading CSV file: {file_path}')
    csv_table = (spark.read
        .format('csv')
        .option('header', 'true')
        .load(file_path)
    )
    print('Finished reading CSV file\n')
    return csv_table



# %%




