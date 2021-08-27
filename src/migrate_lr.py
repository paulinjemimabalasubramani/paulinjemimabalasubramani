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
from modules.spark_functions import add_md5_key, create_spark, read_sql, read_csv
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, read_tableinfo, get_azure_sp, \
    container_name, file_format, to_storage_account_name, tableinfo_name, tableinfo_container_name
from modules.data_functions import to_string, remove_column_spaces, add_elt_columns, execution_date, partitionBy
from modules.data_type_translation import prepare_tableinfo, get_DataTypeTranslation_table, get_files_meta, \
    create_master_ingest_list, get_sql_schema_tables_from_files


from pyspark.sql import functions as F


# %% Logging
logger = make_logging(__name__)


# %% Parameters

ingest_from_files_flag = True

sql_server = 'TSQLOLTP01'
sql_key_vault_account = sql_server

storage_account_name = to_storage_account_name()
domain_name = 'financial_professional'

reception_date = execution_date
tableinfo_source = 'LR'

data_type_translation_id = 'sqlserver_snowflake'

data_path_folder = os.path.join(data_path, tableinfo_source)




# %% Create Session

spark = create_spark()



# %% check if ingest_from_files

if ingest_from_files_flag:
    files_meta = get_files_meta(data_path_folder=data_path_folder, default_schema=tableinfo_source)
    if not files_meta:
        print('No files found, exiting program.')
        exit()

    storage_account_name = to_storage_account_name()
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    master_ingest_list = create_master_ingest_list(spark=spark, files_meta=files_meta)
    translation = get_DataTypeTranslation_table(spark=spark, data_type_translation_id=data_type_translation_id)
    schema_tables = get_sql_schema_tables_from_files(spark=spark, files_meta=files_meta, tableinfo_source=tableinfo_source, master_ingest_list=master_ingest_list)




# %%


if ingest_from_files_flag:
    tableinfo = prepare_tableinfo(
        master_ingest_list = master_ingest_list,
        translation = translation,
        sql_tables = schema_tables['TABLES'],
        sql_columns = schema_tables['COLUMNS'],
        sql_table_constraints = schema_tables['TABLE_CONSTRAINTS'],
        sql_key_column_usage = schema_tables['KEY_COLUMN_USAGE'],
        storage_account_name = storage_account_name,
        tableinfo_source = tableinfo_source,
        )

    save_adls_gen2(
            table_to_save = tableinfo,
            storage_account_name = storage_account_name,
            container_name = tableinfo_container_name,
            container_folder = tableinfo_source,
            table_name = tableinfo_name,
            partitionBy = partitionBy,
            file_format = file_format,
        )




# %% Read metadata.TableInfo

tableinfo, table_rows = read_tableinfo(spark, tableinfo_name=tableinfo_name, tableinfo_source=tableinfo_source)

print('Check if there is a table with no primary key')
nopk = tableinfo.groupBy(['SourceDatabase', 'SourceSchema', 'TableName']).agg(F.sum('KeyIndicator').alias('key_count')).where(F.col('key_count')==F.lit(0))
if is_pc: nopk.show()
assert nopk.count() == 0, 'Found tables with no primary keys'



# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Read SQL Config

if not ingest_from_files_flag:
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

        if ingest_from_files_flag:
            file_path = [file_meta for file_meta in files_meta if file_meta['table'].upper()==table.upper() and file_meta['schema'].upper()==schema.upper()][0]['path']
            sql_table = read_csv(spark=spark, file_path=file_path)
            sql_table = add_md5_key(sql_table)
        else:
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

