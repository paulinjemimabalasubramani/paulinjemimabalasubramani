"""
Migrate all tables from metadata.TableInfo to the ADLS Gen 2

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
from modules.spark_functions import create_spark, read_sql
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, read_tableinfo, get_azure_sp, \
    container_name, file_format, to_storage_account_name, tableinfo_name
from modules.data_functions import to_string, remove_column_spaces, add_elt_columns, execution_date, partitionBy, is_pc
from modules.data_type_translation import prepare_tableinfo


# %% Logging
logger = make_logging(__name__)


# %% Parameters

ingest_from_files = True

sql_server = 'TSQLOLTP01'
sql_key_vault_account = sql_server

storage_account_name = to_storage_account_name()
domain_name = 'financial_professional'

reception_date = execution_date
tableinfo_source = 'LR'


# %% Get Paths

python_dirname = os.path.dirname(__file__)
print(f'Main Path: {os.path.realpath(python_dirname)}')

if is_pc:
    data_path_folder = os.path.realpath(python_dirname + f'/../../Shared/{tableinfo_source}')
else:
    # /usr/local/spark/resources/fileshare/
    data_path_folder = os.path.realpath(python_dirname + f'/../resources/fileshare/Shared/{tableinfo_source}')



# %% Create Session

spark = create_spark()


# %% check if ingest_from_files

if ingest_from_files:










    tableinfo = prepare_tableinfo(
        master_ingest_list = master_ingest_list,
        translation = translation,
        sql_tables = sql_tables,
        sql_columns = sql_columns,
        sql_table_constraints = sql_table_constraints,
        sql_key_column_usage = sql_key_column_usage,
        storage_account_name = storage_account_name,
        )






# %% Read metadata.TableInfo

tableinfo, table_rows = read_tableinfo(spark, tableinfo_name=tableinfo_name, tableinfo_source=tableinfo_source)


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Read SQL Config

_, sql_id, sql_pass = get_azure_sp(sql_key_vault_account.lower())



# %% Loop over all tables

@catch_error(logger)
def iterate_over_all_tables(table_rows):
    table_count = len(table_rows)

    for i, r in enumerate(table_rows):
        table = r['TableName']
        schema = r['SourceSchema']
        database = r['SourceDatabase']

        print(f"\nTable {i+1} of {table_count}: {schema}.{table}")

        data_type = 'data'
        container_folder = f"{data_type}/{domain_name}/{database}/{schema}"

        sql_table = read_sql(spark=spark, user=sql_id, password=sql_pass, schema=schema, table_name=table, database=database, server=sql_server)
        sql_table = to_string(sql_table, col_types = ['timestamp']) # Convert timestamp's to string - as it cause errors otherwise.
        sql_table = remove_column_spaces(sql_table)
        sql_table = add_elt_columns(
            table_to_add = sql_table,
            reception_date = reception_date,
            source = tableinfo_source,
            is_full_load = True,
            dml_type = 'I',
            )

        save_adls_gen2(
            table_to_save = sql_table,
            storage_account_name = storage_account_name,
            container_name = container_name,
            container_folder = container_folder,
            table_name = table,
            partitionBy = partitionBy,
            file_format = file_format
        )
    
    print('Finished Migrating All Tables')



iterate_over_all_tables(table_rows)



# %%

