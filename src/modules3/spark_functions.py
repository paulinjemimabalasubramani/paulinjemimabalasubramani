"""
Library for creating a Spark session and for other common Spark functions


Download SQL Server JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15

https://github.com/microsoft/sql-spark-connector

https://spark.apache.org/docs/latest/configuration

"""

# %% libraries
import os, re

from .common_functions import logger, catch_error, is_pc, get_extraClassPath, execution_date, EXECUTION_DATE_str, data_settings, ELT_PROCESS_ID_str, name_regex

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, lit, concat_ws, coalesce, trim, explode, sha2
from pyspark.sql.types import IntegerType



# %% Parameters

IDKeyIndicator = 'elt_primary_key'

ELT_SOURCE_str = 'elt_source'
ELT_LOAD_TYPE_str = 'elt_load_type'
ELT_DELETE_IND_str = 'elt_delete_ind'
DML_TYPE_str = 'elt_dml_type'
KEY_DATETIME_str = 'elt_reception_date'
ELT_FIRM_str = 'elt_firm'
ELT_PipelineKey_str = 'elt_pipelinekey'

partitionBy_str = 'elt_partition_date'
delta_partitionBy = lambda value: f'{partitionBy_str}={value}'

elt_audit_columns = [EXECUTION_DATE_str, ELT_SOURCE_str, ELT_LOAD_TYPE_str, ELT_DELETE_IND_str, DML_TYPE_str, KEY_DATETIME_str, ELT_PROCESS_ID_str, ELT_FIRM_str, ELT_PipelineKey_str]
elt_audit_columns = [c.lower() for c in elt_audit_columns]



# %% Add ELT Audit Columns

@catch_error(logger)
def add_elt_columns(table, file_meta:dict, dml_type:str):
    """
    Add ELT Audit Columns to the table
    """
    elt_load_type = 'FULL' if file_meta['is_full_load'] else 'INCREMENTAL'

    table = table.withColumn(KEY_DATETIME_str, lit(str(file_meta['key_datetime'])))
    table = table.withColumn(EXECUTION_DATE_str, lit(str(execution_date)))
    table = table.withColumn(ELT_SOURCE_str, lit(str(data_settings.pipeline_datasourcekey)))
    table = table.withColumn(ELT_PipelineKey_str, lit(str(data_settings.pipelinekey)))
    table = table.withColumn(ELT_LOAD_TYPE_str, lit(str(elt_load_type)))
    table = table.withColumn(ELT_DELETE_IND_str, lit(0).cast(IntegerType()))
    table = table.withColumn(ELT_PROCESS_ID_str, lit(data_settings.elt_process_id))
    table = table.withColumn(ELT_FIRM_str, lit(data_settings.pipeline_firm))

    dml_type = dml_type.upper()
    if dml_type not in ['U', 'I', 'D']:
        raise ValueError(f'{DML_TYPE_str} = {dml_type}')

    table = table.withColumn(DML_TYPE_str, lit(str(dml_type)))
    table = table.withColumn(partitionBy_str, lit(str(file_meta['partition_by'])))
    return table



# %% Create Spark

@catch_error(logger)
def create_spark():
    """
    Initiate a new spark session
    """
    def add_from_data_settings(spark, data_settings_name, spark_config_name):
        if hasattr(data_settings, data_settings_name) and getattr(data_settings, data_settings_name):
            data_settings_value = str(getattr(data_settings, data_settings_name))
            logger.info(f'Using {data_settings_name}: {data_settings_value}')
            spark = spark.config(spark_config_name, data_settings_value)
        else:
            logger.info(f'{data_settings_name} config is NOT given. Using system default {data_settings_name}.')
        return spark

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
        .config('spark.executor.heartbeatInterval', '250000')
        .config('spark.network.timeout', '400000')
        )

    spark_data_settings = {
        'spark_master': 'spark.master',
        'spark_executor_instances': 'spark.executor.instances',
        'spark_master_ip': 'spark.executor.host',
        }

    for data_settings_name, spark_config_name in spark_data_settings.items():
        spark = add_from_data_settings(spark=spark, data_settings_name=data_settings_name, spark_config_name=spark_config_name)

    if is_pc:
        drivers_path = os.path.realpath(os.path.dirname(__file__) + '/../../drivers')
        extraClassPath = get_extraClassPath(drivers_path=drivers_path)
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
    spark.sparkContext.setLogLevel('WARN')

    logger.info({
        'Spark_Version': spark.version,
        'Hadoop_Version': spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
    })
    return spark



# %% Convert timestamp's or other types to string

@catch_error(logger)
def to_string(table_to_convert_columns, col_types=['timestamp']):
    """
    Convert timestamp's or other types to string - as it cause errors otherwise.
    """
    string_type = 'string'
    for col_name, col_type in table_to_convert_columns.dtypes:
        if (not col_types or col_type.lower() in col_types) and (col_type.lower() != string_type.lower()):
            table_to_convert_columns = table_to_convert_columns.withColumn(col_name, col(col_name).cast(string_type))

    return table_to_convert_columns



# %% Remove Column Spaces

@catch_error(logger)
def remove_column_spaces(table):
    """
    Removes spaces from column names
    """
    table = table.select([col(f'`{c}`').alias(re.sub(name_regex, '_', c.strip())) for c in table.columns])
    table = to_string(table, col_types = ['timestamp'])
    return table



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

    if sql_table.rdd.isEmpty():
        logger.warning(f'SQL table is empty. Skipping {database}.{sql_table_name}')
        return

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

    table = (spark.read
        .format('snowflake')
        .options(**sf_options)
        .option(dbtable, table_name)
        .load()
    )

    if table.rdd.isEmpty():
        logger.warning(f'Snowflake table is empty. Skipping {database}.{schema}.{table_name}')
        return

    logger.info('Finished Reading from Snowflake')
    return table



# %% Write PySpark table directly to Snowflake

@catch_error(logger)
def write_snowflake(table, table_name:str, schema:str, database:str, warehouse:str, role:str, account:str, user:str, password:str, mode:str='overwrite'):
    """
    Write PySpark table directly to Snowflake
    """
    logger.info(f"Writing to Snowflake: role='{role}', warehouse='{warehouse}', database='{database}', schema='{schema}', table='{table_name}'")

    sf_options = {
        'sfUrl': f'{account}.snowflakecomputing.com',
        'sfUser': user,
        'sfPassword': password,
        'sfRole': role,
        'sfWarehouse': warehouse,
        'sfDatabase': database,
        'sfSchema': schema,
        }

    table.write.mode(mode) \
        .format('snowflake') \
        .options(**sf_options) \
        .option('dbtable', table_name) \
        .save()

    logger.info(f'Finished Writing {database}.{schema}.{table_name} to Snowflake')



# %% Read XML File

@catch_error(logger)
def read_xml(spark, file_path:str, rowTag:str='?xml', schema=None):
    """
    Read XML Files using PySpark
    """
    logger.info(f'Reading XML file: {file_path}')
    xml_table = (spark.read
        .format('com.databricks.spark.xml')
        .option('rowTag', rowTag)
        .option('inferSchema', 'false')
        .option('excludeAttribute', 'false')
        .option('ignoreSurroundingSpaces', 'true')
        .option('mode', 'PERMISSIVE')
    )

    if schema:
        xml_table = xml_table.schema(schema=schema)

    xml_table = xml_table.load(file_path)

    if xml_table.rdd.isEmpty():
        logger.warning(f'XML file is empty. Skipping {file_path}')
        return

    logger.info('Finished reading XML file')
    return xml_table



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

    with open(file=file_path, mode='rt', encoding='utf-8-sig', errors='ignore') as f:
        HEADER = f.readline()

    if '|' in HEADER:
        delimiter = '|'
    elif '\t' in HEADER:
        delimiter = '\t'
    else:
        delimiter = ','

    csv_table = (spark.read # https://github.com/databricks/spark-csv
        .format('csv')
        .option('header', 'true')
        .option('multiLine', 'true')
        .option('delimiter', delimiter) # same as sep
        .option('escape', '"')
        .option('quote', '"')
        .option('charset', 'UTF-8')
        .option('comment', None)
        .option('mode', 'PERMISSIVE')
        .option('parserLib', 'commons')
        .option('inferSchema','false')
        .load(file_path)
    )

    if csv_table.rdd.isEmpty():
        logger.warning(f'CSV file is empty. Skipping {file_path}')
        return

    csv_table = remove_column_spaces(table=csv_table)
    csv_table = trim_string_columns(table=csv_table)

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
        .option('mode', 'PERMISSIVE')
        .load(file_path)
    )

    if text_file.rdd.isEmpty():
        logger.warning(f'Text file is empty. Skipping {file_path}')
        return

    logger.info('Finished reading text file')
    return text_file



# %% Read JSON File

@catch_error(logger)
def read_json(spark, file_path:str):
    """
    Read JSON File using PySpark
    """
    logger.info(f'Reading JSON file: {file_path}')

    json_table = (spark.read
        .format('json')
        .option('multiLine', 'true')
        .option('mode', 'PERMISSIVE')
        .load(file_path)
    )

    if json_table.rdd.isEmpty():
        logger.warning(f'JSON file is empty. Skipping {file_path}')
        return

    logger.info('Finished reading JSON file')
    return json_table



# %% Add ID Key

@catch_error(logger)
def add_id_key(table, key_column_names:list=[], always_use_hash:bool=True):
    """
    Add ID Key to the table
    """
    use_hash = always_use_hash
    if not key_column_names:
        key_column_names = table.columns
        use_hash = True

    id_key_list = [coalesce(col(c).cast('string'), lit('')) for c in key_column_names]
    id_key_list.append(lit(data_settings.pipeline_datasourcekey))

    id_key = concat_ws('_', *id_key_list)
    if use_hash: id_key = sha2(id_key, 256)

    table = table.withColumn(IDKeyIndicator, id_key.alias(IDKeyIndicator, metadata={'maxlength': 1000, 'sqltype': 'varchar(1000)'}))
    return table



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
        if is_pc: print(f'\n{table_select.columns}')
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



# %%




