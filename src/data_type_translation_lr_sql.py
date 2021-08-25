"""
Create/Update metadata.TableInfo table for LR

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

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
from modules.data_functions import execution_date, metadata_DataTypeTranslation, metadata_MasterIngestList, \
    partitionBy
from modules.data_type_translation import join_master_ingest_list_sql_tables, filter_columns_by_tables, join_tables_with_constraints, \
    rename_columns, add_TargetDataType, add_precision


from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit



# %% Logging
logger = make_logging(__name__)


# %% Parameters

data_type_translation_id = 'sqlserver_snowflake'

sql_server = 'TSQLOLTP01'
sql_database = 'LR' # TABLE_CATALOG

tableinfo_source = sql_database

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
    table_name = metadata_MasterIngestList,
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
    table_name = metadata_DataTypeTranslation,
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

sql_tables = read_sql(spark=spark, user=sql_id, password=sql_pass, schema='INFORMATION_SCHEMA', table_name='TABLES', database=sql_database, server=sql_server)
if is_pc: sql_tables.printSchema()
if is_pc: sql_tables.show(5)

sql_columns = read_sql(spark=spark, user=sql_id, password=sql_pass, schema='INFORMATION_SCHEMA', table_name='COLUMNS', database=sql_database, server=sql_server)
if is_pc: sql_columns.printSchema()
if is_pc: sql_columns.show(5)

sql_table_constraints = read_sql(spark=spark, user=sql_id, password=sql_pass, schema='INFORMATION_SCHEMA', table_name='TABLE_CONSTRAINTS', database=sql_database, server=sql_server)
if is_pc: sql_table_constraints.printSchema()
if is_pc: sql_table_constraints.show(5)

sql_key_column_usage = read_sql(spark=spark, user=sql_id, password=sql_pass, schema='INFORMATION_SCHEMA', table_name='KEY_COLUMN_USAGE', database=sql_database, server=sql_server)
if is_pc: sql_key_column_usage.printSchema()
if is_pc: sql_key_column_usage.show(5)



# %% Process Columns

# Join master_ingest_list with sql tables
tables = join_master_ingest_list_sql_tables(master_ingest_list=master_ingest_list, sql_tables=sql_tables)

# filter columns by selected tables
columns = filter_columns_by_tables(sql_columns=sql_columns, tables=tables)

# Join with table constraints and column usage
columns = join_tables_with_constraints(columns=columns, sql_table_constraints=sql_table_constraints, sql_key_column_usage=sql_key_column_usage)

# Rename Columns
columns = rename_columns(columns=columns, storage_account_name=storage_account_name, created_datetime=created_datetime, modified_datetime=modified_datetime)

# Add TargetDataType
columns = add_TargetDataType(columns=columns, translation=translation)

# Add Precision
columns = add_precision(columns=columns)

# Select Relevant columns only
columns = select_tableinfo_columns(columns=columns)



# %% Table Info to ADLS Gen 2

@catch_error(logger)
def save_table_info_to_adls_gen2(columns):
    save_adls_gen2(
            table_to_save = columns,
            storage_account_name = storage_account_name,
            container_name = tableinfo_container_name,
            container_folder = tableinfo_source,
            table_name = tableinfo_name,
            partitionBy = partitionBy,
            file_format = file_format,
        )


save_table_info_to_adls_gen2(columns)



# %%



