"""
Create/Update metadata.TableInfo table for LR

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries
import os, sys
from collections import defaultdict

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.config import is_pc
from modules.spark_functions import create_spark, read_sql
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name, \
    get_azure_sp, file_format, tableinfo_container_name, to_storage_account_name
from modules.data_functions import partitionBy
from modules.data_type_translation import prepare_tableinfo, get_DataTypeTranslation_table, get_master_ingest_list


from pyspark.sql.functions import col, lit



# %% Logging
logger = make_logging(__name__)


# %% Parameters

data_type_translation_id = 'sqlserver_snowflake'

sql_server = 'TSQLOLTP01'
sql_database = 'LR' # TABLE_CATALOG

tableinfo_source = sql_database

INFORMATION_SCHEMA = 'INFORMATION_SCHEMA'.upper()


# %% Create Session

spark = create_spark()


# %% Setup spark to ADLS Gen2 connection

storage_account_name = to_storage_account_name()

setup_spark_adls_gen2_connection(spark, storage_account_name)



# %% Get Master Ingest List

master_ingest_list = get_master_ingest_list(spark=spark, tableinfo_source=tableinfo_source)



# %% Get DataTypeTranslation table

translation = get_DataTypeTranslation_table(spark=spark, data_type_translation_id=data_type_translation_id)



# %% Read SQL Config

_, sql_id, sql_pass = get_azure_sp(sql_server.lower())


# %% Get Table and Column Metadata from information_schema

@catch_error(logger)
def get_sql_schema_tables():
    schema_table_names = ['TABLES', 'COLUMNS', 'KEY_COLUMN_USAGE', 'TABLE_CONSTRAINTS']

    schema_tables = defaultdict()
    for schema_table_name in schema_table_names:
        schema_tables[schema_table_name] = read_sql(spark=spark, user=sql_id, password=sql_pass, schema=INFORMATION_SCHEMA, table_name=schema_table_name, database=sql_database, server=sql_server)
        if is_pc: schema_tables[schema_table_name].printSchema()
        if is_pc: schema_tables[schema_table_name].show(5)
    
    return schema_tables



schema_tables = get_sql_schema_tables()



# %% Prepare TableInfo

tableinfo = prepare_tableinfo(
    master_ingest_list = master_ingest_list,
    translation = translation,
    sql_tables = schema_tables['TABLES'],
    sql_columns = schema_tables['COLUMNS'],
    sql_table_constraints = schema_tables['TABLE_CONSTRAINTS'],
    sql_key_column_usage = schema_tables['KEY_COLUMN_USAGE'],
    storage_account_name = storage_account_name,
    )



# %% Table Info to ADLS Gen 2


save_adls_gen2(
        table_to_save = tableinfo,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = tableinfo_source,
        table_name = tableinfo_name,
        partitionBy = partitionBy,
        file_format = file_format,
    )



# %%



