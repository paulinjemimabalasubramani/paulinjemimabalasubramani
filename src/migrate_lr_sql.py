"""
Migrate all tables to the ADLS Gen 2

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


from modules.common_functions import make_logging, data_path, get_secrets
from modules.spark_functions import create_spark
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo, default_storage_account_name, tableinfo_name
from modules.migrate_files import make_tableinfo, iterate_over_all_tables_migration



# %% Logging
logger = make_logging(__name__)


# %% Parameters

ingest_from_files_flag = False

sql_server = 'TSQLOLTP01'
sql_key_vault_account = sql_server

storage_account_name = default_storage_account_name
domain_name = 'financial_professional'

tableinfo_source = 'LR'
sql_database = tableinfo_source # TABLE_CATALOG

data_type_translation_id = 'sqlserver_snowflake'

data_path_folder = os.path.join(data_path, tableinfo_source)



# %% Create Session

spark = create_spark()


# %% Read SQL Config

sql_id, sql_pass = None, None
if not ingest_from_files_flag:
    _, sql_id, sql_pass = get_secrets(sql_key_vault_account.lower())


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

tableinfo, table_rows = read_tableinfo(spark, tableinfo_name=tableinfo_name, tableinfo_source=tableinfo_source)


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Loop over all tables

iterate_over_all_tables_migration(
    spark = spark,
    tableinfo = tableinfo,
    table_rows = table_rows,
    files_meta = files_meta,
    ingest_from_files_flag = ingest_from_files_flag,
    domain_name = domain_name,
    sql_id = sql_id,
    sql_pass = sql_pass,
    sql_server = sql_server,
    storage_account_name = storage_account_name,
    tableinfo_source = tableinfo_source,
    )


# %%

