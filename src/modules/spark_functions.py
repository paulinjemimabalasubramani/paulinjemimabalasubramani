
"""
Library for creating a Spark session and for other common Spark functions


Download SQL Server JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15

https://github.com/microsoft/sql-spark-connector

https://spark.apache.org/docs/latest/configuration

"""

# %% libraries
import os, re

from .common_functions import logger, catch_error, is_pc, extraClassPath, execution_date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, md5, concat_ws, coalesce, trim
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel



# %% Parameters

column_regex = r'[\W]+'

metadata_DataTypeTranslation = 'DataTypeTranslation'
metadata_MasterIngestList = 'MasterIngestList'
metadata_FirmSourceMap = 'FirmSourceMap'

MD5KeyIndicator = 'MD5_KEY'
IDKeyIndicator = 'ID'

elt_audit_columns = ['RECEPTION_DATE', 'EXECUTION_DATE', 'ELT_SOURCE', 'ELT_LOAD_TYPE', 'ELT_DELETE_IND', 'DML_TYPE']
partitionBy = 'PARTITION_DATE'
partitionBy_value = re.sub(column_regex, '_', execution_date)



# %% Add ELT Audit Columns

@catch_error(logger)
def add_elt_columns(table_to_add, reception_date:str, source:str, is_full_load:bool, dml_type:str=None):
    """
    Add ELT Audit Columns
    """
    table_to_add = table_to_add.withColumn('RECEPTION_DATE', lit(str(reception_date)))
    table_to_add = table_to_add.withColumn('EXECUTION_DATE', lit(str(execution_date)))
    table_to_add = table_to_add.withColumn('ELT_SOURCE', lit(str(source)))
    table_to_add = table_to_add.withColumn('ELT_LOAD_TYPE', lit(str("FULL" if is_full_load else "INCREMENTAL")))
    table_to_add = table_to_add.withColumn('ELT_DELETE_IND', lit(0).cast(IntegerType()))

    if is_full_load:
        DML_TYPE = 'I'
    else:
        DML_TYPE = dml_type.upper()

    if DML_TYPE not in ['U', 'I', 'D']:
        raise ValueError(f'DML_TYPE = {DML_TYPE}')

    table_to_add = table_to_add.withColumn('DML_TYPE', lit(str(DML_TYPE)))
    table_to_add = table_to_add.withColumn(partitionBy, lit(str(partitionBy_value)))
    return table_to_add



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
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.databricks.delta.vacuum.parallelDelete.enabled', 'true')
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

    logger.info({
        'Spark_Version': spark.version,
        'Hadoop_Version': spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
    })
    return spark



# %% Remove Column Spaces

@catch_error(logger)
def remove_column_spaces(table_to_remove):
    """
    Removes spaces from column names
    """
    new_table_to_remove = table_to_remove.select([col(c).alias(re.sub(column_regex, '_', c.strip())) for c in table_to_remove.columns])
    return new_table_to_remove



# %% Convert timestamp's or other types to string

@catch_error(logger)
def to_string(table_to_convert_columns, col_types=['timestamp']):
    """
    Convert timestamp's or other types to string - as it cause errors otherwise.
    """
    string_type = 'string'
    for col_name, col_type in table_to_convert_columns.dtypes:
        if (not col_types or col_type in col_types) and (col_type != string_type):
            logger.info(f"Converting {col_name} from '{col_type}' to 'string' type")
            table_to_convert_columns = table_to_convert_columns.withColumn(col_name, col(col_name).cast(string_type))
    
    return table_to_convert_columns



# %% Read SQL Table

@catch_error(logger)
def read_sql(spark, schema:str, table_name:str, database:str, server:str, user:str, password:str):
    """
    Read a table from SQL server
    """
    sql_table_name = f'{schema}.{table_name}'
    logger.info(f"Reading SQL: server='{server}', database='{database}', table='{sql_table_name}'")

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

    sql_table.persist(StorageLevel.MEMORY_AND_DISK)

    return sql_table



# %% Write table to SQL server

@catch_error(logger)
def write_sql(table, table_name:str, schema:str, database:str, server:str, user:str, password:str, mode:str='overwrite'):
    """
    Write table to SQL server
    """
    sql_table_name = f'{schema}.{table_name}'
    logger.info(f"Writing to SQL Server: server='{server}', database='{database}', table='{sql_table_name}'")

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

    logger.info('Finished Writing to SQL Server')



# %% Function to Get Snowflake Table

@catch_error(logger)
def read_snowflake(spark, table_name:str, schema:str, database:str, warehouse:str, role:str, account:str, user:str, password:str, is_query:bool=False):
    logger.info(f"Reading Snowflake: role='{role}', warehouse='{warehouse}', database='{database}', table='{schema}.{table_name}'")
    sf_options = {
        'sfUrl': f'{account}.snowflakecomputing.com',
        'sfUser': user,
        'sfPassword': password,
        'sfRole': role,
        'sfWarehouse': warehouse,
        'sfDatabase': database,
        'sfSchema': schema,
        }

    if is_query:
        dbtable = 'query'
    else:
        dbtable = 'dbtable'

    table = spark.read \
        .format('snowflake') \
        .options(**sf_options) \
        .option(dbtable, table_name) \
        .load()
    table.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info('Finished Reading from Snowflake')
    return table



# %% Read XML File

@catch_error(logger)
def read_xml(spark, file_path:str, rowTag:str="?xml", schema=None):
    """
    Read XML Files using Spark
    """
    logger.info(f'Reading XML file: {file_path}')
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
    xml_table_load.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info('Finished reading XML file')
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
    logger.info(f'Reading CSV file: {file_path}')
    csv_table = (spark.read # https://github.com/databricks/spark-csv
        .format('csv')
        .option('header', 'true')
        .option('multiLine', 'true')
        .option('delimiter', ',') # same as sep
        .option('escape', '"')
        .option('quote', '"')
        .option('charset', 'UTF-8')
        .option('comment', None)
        .option('mode', 'PERMISSIVE')
        .option('parserLib', 'commons')
        .load(file_path)
    )

    csv_table = remove_column_spaces(table_to_remove=csv_table)
    csv_table = trim_string_columns(table=csv_table)
    csv_table.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info('Finished reading CSV file')
    return csv_table



# %% Read Text File

@catch_error(logger)
def read_text(spark, file_path:str):
    """
    Read Text File using Spark
    """
    logger.info(f'Reading text file: {file_path}')

    text_file = (spark.read
        .format('text')
        .load(file_path)
    )

    text_file.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info('Finished reading text file')
    return text_file



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

    table_names = sql_tables.select('TABLE_NAME').distinct().collect()
    table_names = [x['TABLE_NAME'] for x in table_names]

    return table_names, sql_tables



# %%




