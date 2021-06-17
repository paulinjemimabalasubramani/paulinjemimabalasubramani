# %% Import Libraries
import logging
import os, sys
from datetime import datetime

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.config import is_pc
from modules.spark_functions import create_spark, read_csv, read_sql, read_sql_config
from modules.azure_functions import get_azure_storage_key_vault, save_adls_gen2_sp


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window


# %% Logging
logger = make_logging(__name__)


# %% Parameters
data_type_translation_path = os.path.realpath(os.path.dirname(__file__)+'/../metadata_source_files/DataTypeTranslation.csv')
assert os.path.isfile(data_type_translation_path), f"File not found: {data_type_translation_path}"

table_list_path = os.path.realpath(os.path.dirname(__file__)+'/../config/LNR_Tables.csv')
assert os.path.isfile(table_list_path), f"File not found: {table_list_path}"

data_type_translation_id = 'sqlserver_snowflake'

database = 'LR' # TABLE_CATALOG
server = 'TSQLOLTP01'

storage_account_name = "agaggrlakescd"
azure_tenant_id, sp_id, sp_pass = get_azure_storage_key_vault(storage_name=storage_account_name)
container_name = "ingress"
domain_name = 'financial_professional'
format = 'delta'

execution_date = datetime.now()
created_datetime = execution_date
modified_datetime = execution_date

# %% Create Session

spark = create_spark()



# %% Get DataTypeTranslation table

@catch_error(logger)
def get_translation(data_type_translation_path, data_type_translation_id:str):
    """
    Get DataTypeTranslation table
    """
    translation = read_csv(spark, data_type_translation_path)
    translation.printSchema()
    translation = translation.filter(
                        (col('DataTypeTranslationID') == lit(data_type_translation_id).cast("string")) & 
                        (col('IsActive') == lit(1))
                        )

    translation.createOrReplaceTempView('translation')
    return translation



translation = get_translation(data_type_translation_path, data_type_translation_id)

translation.show(5)



# %% Get List of Tables of interest

@catch_error(logger)
def get_table_list(table_list_path:str):
    """
    Get List of Tables of interest
    """
    table_list = read_csv(spark, table_list_path)
    if is_pc: table_list.printSchema()
    table_list = table_list.filter(F.lower(col('Table of Interest')) == lit('yes').cast("string"))

    column_map = {
        'TableName': 'TABLE_NAME',
        'SchemaName' : 'TABLE_SCHEMA',
    }

    for key, val in column_map.items():
        table_list = table_list.withColumnRenamed(key, val)

    table_list.createOrReplaceTempView('table_list')
    return table_list



table_list = get_table_list(table_list_path)

table_list.show(5)

# %% Read SQL Config

sql_config = read_sql_config()


# %% Get Table and Column Metadata from information_schema

sql_tables = read_sql(spark=spark, user=sql_config.sql_user, password=sql_config.sql_password, schema='INFORMATION_SCHEMA', table='TABLES', database=database, server=server)
if is_pc: sql_tables.printSchema()
if is_pc: sql_tables.show(5)

sql_columns = read_sql(spark=spark, user=sql_config.sql_user, password=sql_config.sql_password, schema='INFORMATION_SCHEMA', table='COLUMNS', database=database, server=server)
if is_pc: sql_columns.printSchema()
if is_pc: sql_columns.show(5)

sql_table_constraints = read_sql(spark=spark, user=sql_config.sql_user, password=sql_config.sql_password, schema='INFORMATION_SCHEMA', table='TABLE_CONSTRAINTS', database=database, server=server)
if is_pc: sql_table_constraints.printSchema()
if is_pc: sql_table_constraints.show(5)

sql_key_column_usage = read_sql(spark=spark, user=sql_config.sql_user, password=sql_config.sql_password, schema='INFORMATION_SCHEMA', table='KEY_COLUMN_USAGE', database=database, server=server)
if is_pc: sql_key_column_usage.printSchema()
if is_pc: sql_key_column_usage.show(5)


# %% Join tables of interest with sql tables

@catch_error(logger)
def join_table_list_sql_tables(table_list, sql_tables):
    tables = table_list.join(
        sql_tables,
        (table_list.TABLE_NAME == sql_tables.TABLE_NAME) &
        (table_list.TABLE_SCHEMA == sql_tables.TABLE_SCHEMA),
        how = 'left'
        ).select(
            table_list.TABLE_NAME, 
            table_list.TABLE_SCHEMA,
            sql_tables.TABLE_NAME.alias('SQL_TABLE_NAME'),
            sql_tables.TABLE_TYPE,
            sql_tables.TABLE_CATALOG,
        )

    if is_pc: tables.printSchema()
    if is_pc: tables.show(5)

    # Check if there is a table in the table_list that is not in the sql_tables
    null_rows = tables.filter(col('SQL_TABLE_NAME').isNull()).select(col('TABLE_NAME')).collect()
    assert not null_rows, f"There are some tables in table_list that are not in sql_tables: {[x[0] for x in null_rows]}"

    return tables


tables = join_table_list_sql_tables(table_list, sql_tables)

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
    
    columns = columns.withColumn('IsNullable', F.when(F.upper(col('IS_NULLABLE'))=='YES', lit(1)).otherwise(lit(0)))
    columns = columns.withColumn('KeyIndicator', F.when((F.upper(col('CONSTRAINT_TYPE'))=='PRIMARY KEY') & (col('SourceColumnName')==col('KEY_COLUMN_NAME')), lit(1)).otherwise(lit(0)))
    columns = columns.withColumn('CleanType', col('SourceDataType'))
    columns = columns.withColumn('TargetColumnName', col('SourceColumnName'))

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
            'columns.*', translation.DataTypeTo.alias('TargetDataType')
        )

    if is_pc: columns.printSchema()
    return columns


columns = add_TargetDataType(columns, translation)

# %%




