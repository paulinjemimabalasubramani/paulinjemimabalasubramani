# %% Import Libraries
import os, sys

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.config import is_pc
from modules.spark_functions import create_spark, read_sql
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name, read_adls_gen2, \
    get_azure_sp, file_format, tableinfo_container_name, to_storage_account_name, select_tableinfo_columns
from modules.data_functions import execution_date, column_regex, metadata_DataTypeTranslation, metadata_MasterIngestList, \
    partitionBy, partitionBy_value


from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StringType


# %% Logging
logger = make_logging(__name__)


# %% Parameters

data_type_translation_id = 'sqlserver_snowflake'

database = 'LR' # TABLE_CATALOG
sql_server = 'TSQLOLTP01'
tableinfo_source = database

created_datetime = execution_date
modified_datetime = execution_date


# %% Create Session

spark = create_spark()


# %% Setup spark to ADLS Gen2 connection

storage_account_name = to_storage_account_name()

setup_spark_adls_gen2_connection(spark, storage_account_name)



# %% Get Master Ingest List

master_ingest_list = read_adls_gen2(
    spark = spark,
    storage_account_name = storage_account_name,
    container_name = tableinfo_container_name,
    container_folder = tableinfo_source,
    table = metadata_MasterIngestList,
    file_format = file_format
)

master_ingest_list = master_ingest_list.filter(
    col('IsActive')==lit(1)
)

if is_pc: master_ingest_list.show(5)



# %% Get DataTypeTranslation table

translation = read_adls_gen2(
    spark = spark,
    storage_account_name = storage_account_name,
    container_name = tableinfo_container_name,
    container_folder = '',
    table = metadata_DataTypeTranslation,
    file_format = file_format
)

translation = translation.filter(
    (col('DataTypeTranslationID') == lit(data_type_translation_id).cast("string")) & 
    (col('IsActive') == lit(1))
)

if is_pc: translation.show(5)



# %% Read SQL Config

_, sql_id, sql_pass = get_azure_sp(sql_server.lower())


# %% Get Table and Column Metadata from information_schema

sql_tables = read_sql(spark=spark, user=sql_id, password=sql_pass, schema='INFORMATION_SCHEMA', table='TABLES', database=database, server=sql_server)
if is_pc: sql_tables.printSchema()
if is_pc: sql_tables.show(5)

sql_columns = read_sql(spark=spark, user=sql_id, password=sql_pass, schema='INFORMATION_SCHEMA', table='COLUMNS', database=database, server=sql_server)
if is_pc: sql_columns.printSchema()
if is_pc: sql_columns.show(5)

sql_table_constraints = read_sql(spark=spark, user=sql_id, password=sql_pass, schema='INFORMATION_SCHEMA', table='TABLE_CONSTRAINTS', database=database, server=sql_server)
if is_pc: sql_table_constraints.printSchema()
if is_pc: sql_table_constraints.show(5)

sql_key_column_usage = read_sql(spark=spark, user=sql_id, password=sql_pass, schema='INFORMATION_SCHEMA', table='KEY_COLUMN_USAGE', database=database, server=sql_server)
if is_pc: sql_key_column_usage.printSchema()
if is_pc: sql_key_column_usage.show(5)



# %% Join master_ingest_list with sql tables

@catch_error(logger)
def join_master_ingest_list_sql_tables(master_ingest_list, sql_tables):
    tables = master_ingest_list.join(
        sql_tables,
        (master_ingest_list.TABLE_NAME == sql_tables.TABLE_NAME) &
        (master_ingest_list.TABLE_SCHEMA == sql_tables.TABLE_SCHEMA),
        how = 'left'
        ).select(
            master_ingest_list.TABLE_NAME, 
            master_ingest_list.TABLE_SCHEMA,
            sql_tables.TABLE_NAME.alias('SQL_TABLE_NAME'),
            sql_tables.TABLE_TYPE,
            sql_tables.TABLE_CATALOG,
        )

    if is_pc: tables.printSchema()
    if is_pc: tables.show(5)

    # Check if there is a table in the master_ingest_list that is not in the sql_tables
    null_rows = tables.filter(col('SQL_TABLE_NAME').isNull()).select(col('TABLE_NAME')).collect()
    assert not null_rows, f"There are some tables in master_ingest_list that are not in sql_tables: {[x[0] for x in null_rows]}"

    return tables


tables = join_master_ingest_list_sql_tables(master_ingest_list, sql_tables)

# %% filter columns by selected tables

@catch_error(logger)
def filter_columns_by_tables(sql_columns, tables):
    columns = tables.join(
        sql_columns.alias('sql_columns'),
        (tables.TABLE_NAME == sql_columns.TABLE_NAME) &
        (tables.TABLE_SCHEMA == sql_columns.TABLE_SCHEMA) &
        (tables.TABLE_CATALOG == sql_columns.TABLE_CATALOG),
        how = 'left'
    ).select('sql_columns.*')

    if is_pc: columns.printSchema()
    return columns



columns = filter_columns_by_tables(sql_columns, tables)



# %% Join with table constraints and column usage

@catch_error(logger)
def join_tables_with_constraints(columns, sql_table_constraints, sql_key_column_usage):
    columns = columns.alias('columns').join(
        sql_table_constraints,
        (columns.TABLE_NAME == sql_table_constraints.TABLE_NAME) &
        (columns.TABLE_SCHEMA == sql_table_constraints.TABLE_SCHEMA) &
        (columns.TABLE_CATALOG == sql_table_constraints.TABLE_CATALOG) &
        (sql_table_constraints.CONSTRAINT_TYPE == 'PRIMARY KEY'),
        how = 'left'
        ).select(
            'columns.*', 
            sql_table_constraints.CONSTRAINT_TYPE, 
            sql_table_constraints.CONSTRAINT_NAME
            )
    
    columns = columns.alias('columns').join(
        sql_key_column_usage,
        (columns.TABLE_NAME == sql_key_column_usage.TABLE_NAME) &
        (columns.TABLE_SCHEMA == sql_key_column_usage.TABLE_SCHEMA) &
        (columns.TABLE_CATALOG == sql_key_column_usage.TABLE_CATALOG) &
        (columns.COLUMN_NAME == sql_key_column_usage.COLUMN_NAME) &
        (columns.CONSTRAINT_NAME == sql_key_column_usage.CONSTRAINT_NAME)
        ,
        how = 'left'
        ).select(
            'columns.*',
            sql_key_column_usage.COLUMN_NAME.alias('KEY_COLUMN_NAME')
        )
    
    if is_pc: columns.printSchema()
    return columns



columns = join_tables_with_constraints(columns, sql_table_constraints, sql_key_column_usage)


# %% Rename Columns

@catch_error(logger)
def rename_columns(columns):
    column_map = {
        'TABLE_CATALOG': 'SourceDatabase',
        'TABLE_SCHEMA' : 'SourceSchema',
        'TABLE_NAME'   : 'TableName',
        'COLUMN_NAME'  : 'SourceColumnName',
        'DATA_TYPE'    : 'SourceDataType',
        'CHARACTER_MAXIMUM_LENGTH': 'SourceDataLength',
        'NUMERIC_PRECISION': 'SourceDataPrecision',
        'NUMERIC_SCALE': 'SourceDataScale',
        'ORDINAL_POSITION': 'OrdinalPosition',
    }

    for key, val in column_map.items():
        columns = columns.withColumnRenamed(key, val)
    
    columns = columns.withColumn('IsNullable', F.when(F.upper(col('IS_NULLABLE'))=='YES', lit(1)).otherwise(lit(0)).cast(IntegerType()))
    columns = columns.withColumn('KeyIndicator', F.when((F.upper(col('CONSTRAINT_TYPE'))=='PRIMARY KEY') & (col('SourceColumnName')==col('KEY_COLUMN_NAME')), lit(1)).otherwise(lit(0)).cast(IntegerType()))
    columns = columns.withColumn('CleanType', col('SourceDataType'))
    columns = columns.withColumn('TargetColumnName', F.regexp_replace(col('SourceColumnName'), column_regex, '_'))
    columns = columns.withColumn('IsActive', lit(1).cast(IntegerType()))
    columns = columns.withColumn('CreatedDateTime', lit(created_datetime).cast(StringType()))
    columns = columns.withColumn('ModifiedDateTime', lit(modified_datetime).cast(StringType()))
    columns = columns.withColumn(partitionBy, lit(partitionBy_value).cast(StringType()))

    if is_pc: columns.printSchema()
    return columns


columns = rename_columns(columns)


# %% Add TargetDataType

@catch_error(logger)
def add_TargetDataType(columns, translation):
    columns = columns.alias('columns').join(
        translation,
        columns.CleanType == translation.DataTypeFrom,
        how = 'left'
        ).select(
            'columns.*', 
            translation.DataTypeTo.alias('TargetDataType')
        )

    if is_pc: columns.printSchema()
    return columns


columns = add_TargetDataType(columns, translation)


# %% Add Precision

@catch_error(logger)
def add_precision(columns):
    columns = columns.withColumn('TargetDataType', F.when((col('TargetDataType').isin(['varchar'])) & (col('SourceDataLength')>0) & (col('SourceDataLength')<=255), F.concat(lit('varchar('), col('SourceDataLength'), lit(')'))).otherwise(col('TargetDataType')))
    columns = columns.withColumn('TargetDataType', F.when((col('TargetDataType').isin(['decimal'])) & (col('SourceDataPrecision')>0), F.concat(lit('decimal('), col('SourceDataPrecision'), lit(','), col('SourceDataScale'), lit(')'))).otherwise(col('TargetDataType')))

    if is_pc: columns.printSchema()
    return columns
    


columns = add_precision(columns)
columns = select_tableinfo_columns(columns)


# %% Table Info to ADLS Gen 2

@catch_error(logger)
def save_table_info_to_adls_gen2(columns):
    save_adls_gen2(
            table_to_save = columns,
            storage_account_name = storage_account_name,
            container_name = tableinfo_container_name,
            container_folder = tableinfo_source,
            table = tableinfo_name,
            partitionBy = partitionBy,
            file_format = file_format,
        )


save_table_info_to_adls_gen2(columns)



# %%



