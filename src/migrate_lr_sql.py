"""
Migrate all tables to the ADLS Gen 2

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'financial_professional'
sys.domain_abbr = 'FP'


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import data_settings, get_secrets, logger, mark_execution_end
from modules.spark_functions import create_spark
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo, default_storage_account_name, tableinfo_name
from modules.migrate_files import make_tableinfo, iterate_over_all_tables_migration
from modules.snowflake_ddl import connect_to_snowflake, iterate_over_all_tables_snowflake, create_source_level_tables, snowflake_ddl_params



# %% Parameters

ingest_from_files_flag = False

sql_server = 'TSQLOLTP01'
sql_key_vault_account = sql_server

storage_account_name = default_storage_account_name

tableinfo_source = 'LR'
sql_database = tableinfo_source # TABLE_CATALOG

data_type_translation_id = 'sqlserver_snowflake'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))



# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark


# %% Read SQL Config

sql_id, sql_pass = None, None
if not ingest_from_files_flag:
    _, sql_id, sql_pass = get_secrets(sql_key_vault_account.lower(), logger=logger)


# %% Make TableInfo

files_meta, tableinfo = make_tableinfo(
    spark = spark,
    ingest_from_files_flag = ingest_from_files_flag,
    data_path_folder = data_path_folder,
    default_schema = tableinfo_source,
    tableinfo_source = tableinfo_source,
    data_type_translation_id = data_type_translation_id,
    sql_id = sql_id,
    sql_pass = sql_pass,
    sql_server = sql_server,
    sql_database = sql_database,
    )


# %% Read metadata.TableInfo

tableinfo, table_rows = read_tableinfo(spark, tableinfo_name=tableinfo_name, tableinfo_source=tableinfo_source, tableinfo=tableinfo)


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Loop over all tables

PARTITION_list = iterate_over_all_tables_migration(
    spark = spark,
    tableinfo = tableinfo,
    table_rows = table_rows,
    files_meta = files_meta,
    ingest_from_files_flag = ingest_from_files_flag,
    sql_id = sql_id,
    sql_pass = sql_pass,
    sql_server = sql_server,
    storage_account_name = storage_account_name,
    tableinfo_source = tableinfo_source,
    )


# %% Connect to SnowFlake

snowflake_connection = connect_to_snowflake()
snowflake_ddl_params.snowflake_connection = snowflake_connection


# %% Iterate Over Steps for all tables

ingest_data_list = iterate_over_all_tables_snowflake(tableinfo=tableinfo, table_rows=table_rows, PARTITION_list=PARTITION_list)


# %% Create Source Level Tables

create_source_level_tables(ingest_data_list=ingest_data_list)


# %% Close Showflake connection

snowflake_connection.close()


# %% Mark Execution End

mark_execution_end()


# %%

