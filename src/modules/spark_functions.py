
"""
Library for creating a Spark session and for other common Spark functions


Download SQL Server JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15

https://github.com/microsoft/sql-spark-connector

https://spark.apache.org/docs/latest/configuration

"""

# %% libraries
import os, platform


from .common_functions import make_logging, catch_error, system_info, is_pc, extraClassPath
from .data_functions import remove_column_spaces

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, md5, concat_ws, coalesce, trim



# %% Logging
logger = make_logging(__name__)



# %% Parameters

MD5KeyIndicator = 'MD5_KEY'
IDKeyIndicator = 'ID'



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
        .config('spark.sql.optimizer.maxIterations', '300')
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
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
    print(system_info(),'\n')

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
def write_sql(table, table_name:str, schema:str, database:str, server:str, user:str, password:str, mode:str='overwrite'):
    """
    Write table to SQL server
    """
    sql_table_name = f'{schema}.{table_name}'
    print(f"\nWriting to SQL Server: server='{server}', database='{database}', table='{sql_table_name}'")

    url = f'jdbc:sqlserver://{server};databaseName={database};trustServerCertificate=true;'

    table.write.mode(mode) \
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



# %% Trim String Columns

@catch_error(logger)
def trim_string_columns(table):
    for colname, coltype in table.dtypes:
        if coltype.lower() == 'string':
            table = table.withColumn(colname, trim(col(colname)))
    return table



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

    csv_table = remove_column_spaces(table_to_remove=csv_table)
    csv_table = trim_string_columns(table=csv_table)

    print('Finished reading CSV file\n')
    return csv_table



# %% Add ID Key

@catch_error(logger)
def add_id_key(table, key_column_names:list):
    coalesce_list = [coalesce(col(c).cast('string'), lit('')) for c in key_column_names]
    table = table.withColumn(MD5KeyIndicator, concat_ws('_', *coalesce_list)) # TODO: Change this to IDKeyIndicator later when we can add schema change
    return table


# %% Add MD5 Key

@catch_error(logger)
def add_md5_key(table, key_column_names:list=[]):
    if not key_column_names:
        key_column_names = table.columns
    coalesce_list = [coalesce(col(c).cast('string'), lit('')) for c in key_column_names]
    table = table.withColumn(MD5KeyIndicator, md5(concat_ws('_', *coalesce_list)))
    return table



# %% Get SQL Table Names

@catch_error(logger)
def get_sql_table_names(spark, schema:str, database:str, server:str, user:str, password:str):
    sql_tables = read_sql(
        spark = spark, 
        user = user, 
        password = password, 
        schema = 'INFORMATION_SCHEMA', 
        table_name = 'TABLES', 
        database = database, 
        server = server,
        )

    sql_tables = sql_tables.filter((col('TABLE_SCHEMA') == lit(schema)) & (col('TABLE_TYPE')==lit('BASE TABLE')))

    table_names = sql_tables.select('TABLE_NAME').distinct().rdd.flatMap(lambda x: x).collect()

    return table_names, sql_tables



# %%




