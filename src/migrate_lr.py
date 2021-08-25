"""
Migrate all tables from metadata.TableInfo to the ADLS Gen 2

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
from pprint import pprint


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.config import data_path, is_pc
from modules.spark_functions import create_spark, read_sql
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, read_tableinfo, get_azure_sp, \
    container_name, file_format, to_storage_account_name, tableinfo_name
from modules.data_functions import to_string, remove_column_spaces, add_elt_columns, execution_date, partitionBy
from modules.data_type_translation import prepare_tableinfo, get_DataTypeTranslation_table


from pyspark.sql.functions import col, lit



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

data_type_translation_id = 'sqlserver_snowflake'
INFORMATION_SCHEMA = 'INFORMATION_SCHEMA'.upper()

data_path_folder = os.path.join(data_path, tableinfo_source)




# %% Create Session

spark = create_spark()



# %% Get Files Meta

@catch_error(logger)
def get_files_meta():
    files_meta = []
    for root, dirs, files in os.walk(data_path_folder):
        for file in files:
            file_name, file_ext = os.path.splitext(file)
            if (file_ext.lower() in ['.txt', '.csv']) and ('_' in file_name):
                file_meta = {
                    'file': file,
                    'root': root,
                    'path': os.path.join(root, file)
                }

                if file_name.upper().startswith(INFORMATION_SCHEMA + '_'):
                    schema = INFORMATION_SCHEMA
                    table = file_name[len(INFORMATION_SCHEMA)+1:].upper()
                else:
                    _loc = file_name.find("_")
                    schema = file_name[:_loc].lower()
                    table = file_name[_loc+1:].lower()

                file_meta = {
                    **file_meta,
                    'schema': schema,
                    'table': table,
                }

                files_meta.append(file_meta)

    if is_pc: pprint(files_meta)
    return files_meta



# %% Create Master Ingest List

@catch_error(logger)
def create_master_ingest_list(files_meta):
    files_meta_for_master_ingest_list =[{
        'TABLE_SCHEMA': file_meta['schema'],
        'TABLE_NAME': file_meta['table'],
        } for file_meta in files_meta if file_meta['schema'].upper()!=INFORMATION_SCHEMA]

    master_ingest_list = spark.createDataFrame(files_meta_for_master_ingest_list)

    print(f'Total of {master_ingest_list.count()} tables to ingest')
    return master_ingest_list




# %% check if ingest_from_files

if ingest_from_files:
    storage_account_name = to_storage_account_name()
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    files_meta = get_files_meta()
    master_ingest_list = create_master_ingest_list(files_meta=files_meta)
    translation = get_DataTypeTranslation_table(spark=spark, data_type_translation_id=data_type_translation_id)




# %%


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

