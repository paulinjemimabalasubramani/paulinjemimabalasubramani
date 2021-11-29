
"""
Library for creating a Spark session and for other common Spark functions


Download SQL Server JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15

https://github.com/microsoft/sql-spark-connector

https://spark.apache.org/docs/latest/configuration

"""

# %% libraries
import os, re, json
from pprint import pprint

from .common_functions import logger, catch_error, is_pc, extraClassPath, execution_date, EXECUTION_DATE_str

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, lit, md5, concat_ws, coalesce, trim, explode
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel



# %% Parameters

column_regex = r'[\W]+'

metadata_DataTypeTranslation = 'DataTypeTranslation'
metadata_MasterIngestList = 'MasterIngestList'
metadata_FirmSourceMap = 'FirmSourceMap'

MD5KeyIndicator = 'MD5_KEY'
IDKeyIndicator = 'ID'

RECEPTION_DATE_str = 'RECEPTION_DATE'
ELT_SOURCE_str = 'ELT_SOURCE'
ELT_LOAD_TYPE_str = 'ELT_LOAD_TYPE'
ELT_DELETE_IND_str = 'ELT_DELETE_IND'
DML_TYPE_str = 'DML_TYPE'

partitionBy = 'PARTITION_DATE'
partitionBy_value = re.sub(column_regex, '_', execution_date)

elt_audit_columns = [RECEPTION_DATE_str, EXECUTION_DATE_str, ELT_SOURCE_str, ELT_LOAD_TYPE_str, ELT_DELETE_IND_str, DML_TYPE_str]



# %% Add ELT Audit Columns

@catch_error(logger)
def add_elt_columns(table, reception_date:str, source:str, is_full_load:bool, dml_type:str=None):
    """
    Add ELT Audit Columns to the table
    """
    table = table.withColumn(RECEPTION_DATE_str, lit(str(reception_date)))
    table = table.withColumn(EXECUTION_DATE_str, lit(str(execution_date)))
    table = table.withColumn(ELT_SOURCE_str, lit(str(source)))
    table = table.withColumn(ELT_LOAD_TYPE_str, lit(str("FULL" if is_full_load else "INCREMENTAL")))
    table = table.withColumn(ELT_DELETE_IND_str, lit(0).cast(IntegerType()))

    if is_full_load:
        DML_TYPE = 'I'
    else:
        DML_TYPE = dml_type.upper()

    if DML_TYPE not in ['U', 'I', 'D']:
        raise ValueError(f'{DML_TYPE_str} = {DML_TYPE}')

    table = table.withColumn(DML_TYPE_str, lit(str(DML_TYPE)))
    table = table.withColumn(partitionBy, lit(str(partitionBy_value)))
    return table



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



# %% Utility function to collect data from specific PySpark Table column

@catch_error(logger)
def collect_column(table, column_name:str, distinct:bool=True):
    """
    Utility function to collect data from specific PySpark Table column
    """
    table = table.select(column_name)
    if distinct:
        table = table.distinct()
    values = table.collect()
    values = [x[column_name] for x in values]
    return values



# %% Utility function to convert PySpark table to list of dictionaries

@catch_error(logger)
def table_to_list_dict(table):
    """
    Utility function to convert PySpark table to list of dictionaries
    """
    return table.toJSON().map(lambda j: json.loads(j)).collect()



# %% Remove Column Spaces

@catch_error(logger)
def remove_column_spaces(table):
    """
    Removes spaces from column names
    """
    table = table.select([col(f'`{c}`').alias(re.sub(column_regex, '_', c.strip())) for c in table.columns])
    return table



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
    """
    Read from Snowflake Table or Query
    """
    logger.info(f"Reading Snowflake: role='{role}', warehouse='{warehouse}', database='{database}', schema='{schema}', table='{table_name}'")
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
    Read XML Files using PySpark
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

    if is_pc: xml_table_load.show(5)

    logger.info('Finished reading XML file')
    return xml_table_load



# %% Trim String Columns

@catch_error(logger)
def trim_string_columns(table):
    """
    Trim String Columns of PySpark table
    """
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

    csv_table = remove_column_spaces(table=csv_table)
    csv_table = trim_string_columns(table=csv_table)
    csv_table.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info('Finished reading CSV file')
    return csv_table



# %% Read Text File

@catch_error(logger)
def read_text(spark, file_path:str):
    """
    Read Text File using PySpark
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
    """
    Add ID Key to the table
    """
    if not key_column_names:
        logger.warning('No Key Column Names found for creating ID Key')
        return table
 
    coalesce_list = [coalesce(col(c).cast('string'), lit('')) for c in key_column_names]
    table = table.withColumn(MD5KeyIndicator, concat_ws('_', *coalesce_list).alias(MD5KeyIndicator, metadata={'maxlength': 1000, 'sqltype': 'varchar(1000)'})) # TODO: Change this to IDKeyIndicator later when we can add schema change
    return table



# %% Add MD5 Key

@catch_error(logger)
def add_md5_key(table, key_column_names:list=[]):
    """
    Add MD5 Key to the table
    """
    if not key_column_names:
        key_column_names = table.columns
    coalesce_list = [coalesce(col(c).cast('string'), lit('')) for c in key_column_names]
    table = table.withColumn(MD5KeyIndicator, md5(concat_ws('_', *coalesce_list)).alias(MD5KeyIndicator, metadata={'maxlength': 1000, 'sqltype': 'varchar(1000)'}))
    return table



# %% Get SQL Table Names

@catch_error(logger)
def get_sql_table_names(spark, schema:str, database:str, server:str, user:str, password:str):
    """
    Get SQL Table Names
    """
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
    table_names = collect_column(table=sql_tables, column_name='TABLE_NAME', distinct=True)
    return table_names, sql_tables



# %% Convert Base json metadata to Spark Schema type

@catch_error(logger)
def base_to_schema(base:dict):
    """
    Convert Base json metadata to Spark Schema type
    """
    st = []
    for key, val in base.items():
        if val is None:
            v = StringType()
        elif isinstance(val, dict):
            v = base_to_schema(val)
        elif isinstance(val, list):
            v = ArrayType(base_to_schema(val[0]), True)
        else:
            v = val

        st.append(StructField(key, v, True))
    return StructType(st)



# %% Flatten table

@catch_error(logger)
def flatten_table(table):
    """
    Flatten nested PySpark table down to the level where there can be multiple arrays.
    This function does not divide table into sub-tables.
    """
    cols = []
    nested = False

    # to ensure not to have more than 1 explosion in a table
    expolode_flag = len([c[0] for c in table.dtypes if c[1].startswith('array')]) <= 1

    for c in table.dtypes:
        if c[1].startswith('struct'):
            nested = True
            if len(table.columns)>1:
                struct_cols = table.select(c[0]+'.*').columns
                cols.extend([col(c[0]+'.'+cc).alias(c[0]+('' if cc[0]=='_' else '_')+cc) for cc in struct_cols])
            else:
                cols.append(c[0]+'.*')
        elif c[1].startswith('array') and expolode_flag:
            nested = True
            cols.append(explode(c[0]).alias(c[0]))
        else:
            cols.append(c[0])

    table_select = table.select(cols)
    if nested:
        if is_pc: pprint(f'\n{table_select.columns}')
        table_select = flatten_table(table_select)

    return table_select



# %% flatten and divide

@catch_error(logger)
def flatten_n_divide_table(table, table_name:str):
    """
    Flatten the table first, then divide it one array per table if the flat table has arrays. 
    Then repeat the flattening process in the sub-tables until all the tables are fully flat.
    """
    if table.rdd.isEmpty(): # check if table is empty
        return dict()

    table = flatten_table(table=table)

    string_cols = [c[0] for c in table.dtypes if c[1]=='string']
    array_cols = [c[0] for c in table.dtypes if c[1].startswith('array')]

    if len(array_cols)==0:
        return {table_name:table}

    table_list = dict()
    for array_col in array_cols:
        colx = string_cols + [array_col]
        table_select = table.select(*colx)
        table_list={**table_list, **flatten_n_divide_table(table=table_select, table_name=table_name+'_'+array_col)}

    return table_list



# %% Get List of Columns from "Columns" table order by OrdinalPosition

@catch_error(logger)
def get_columns_list_from_columns_table(columns, column_names:list, OrdinalPosition:str=None):
    select_columns = column_names.copy()
    if OrdinalPosition:
        select_columns += [OrdinalPosition]
    select_columns = list(set(select_columns))
    column_list = columns.select(select_columns).distinct().collect()

    if OrdinalPosition:
        column_list = [({x:c[x] for x in column_names}, c[OrdinalPosition]) for c in column_list]
    else:
        column_list = [({x:c[x] for x in column_names}, (c[x] for x in column_names)) for c in column_list]

    column_list = sorted(column_list, key=lambda x: x[1])
    column_list = [x[0] for x in column_list]

    return column_list



# %%




