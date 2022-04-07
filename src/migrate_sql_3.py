description = """

Generic Code to Migrate SQL Server Tables to ADLS Gen 2

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'FP_MIGRATE_LR',
        'source_path': r'C:\myworkdir\Shared\LR',
        'sql_table_list': r'C:\myworkdir\EDIP-Code\config\lookup_files\LNR_Tables.csv',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, execution_date_start, get_csv_rows, get_secrets
from modules3.spark_functions import add_id_key, create_spark, read_csv, remove_column_spaces, add_elt_columns, read_sql
from modules3.migrate_files import migrate_all_files, get_key_column_names, default_table_dtypes, add_firm_to_table_name



# %% Parameters

_, sql_user, sql_pass = get_secrets(data_settings.sql_key_vault_account.lower(), logger=logger)

error_tables = []

len_table_list = 0



# %% Create Connections

spark = create_spark()



# %% Select Tables

@catch_error(logger)
def select_sql_tables():
    """
    Get list of tables to ingest
    """
    global len_table_list
    table_list = []

    for row in get_csv_rows(csv_file_path=data_settings.sql_table_list):
        if row['table_of_interest'].upper() in ['Y', 'YES', '1']:
            full_table_name = f"{row['schemaname']}.{row['tablename']}".lower()
            table_list.append(full_table_name)

    len_table_list = len(table_list)
    return len(table_list), table_list



# %% Extract Meta Data from SQL Table

@catch_error(logger)
def extract_sql_table_meta(file_path:str, zip_file_path:str=None):
    """
    Extract Meta Data from SQL Table
    """
    table_name = file_path.lower()
    table_name = add_firm_to_table_name(table_name=table_name)
    key_datetime = execution_date_start

    table_meta = {
        'table_name': table_name.lower(), # table name should always be lower
        'file_name': '',
        'file_path': '',
        'folder_path': '',
        'zip_file_path': zip_file_path,
        'is_full_load': data_settings.is_full_load.upper() == 'TRUE',
        'key_datetime': key_datetime,
    }

    return table_meta



# %% Main Processing of sinlge sql table

@catch_error(logger)
def process_sql_table(file_meta):
    """
    Main Processing of single sql table
    """
    schema_name, table_name = file_meta['table_name'].split('.')
    file_meta['table_name'] = f'{schema_name}_{table_name}'

    try:
        table = read_sql(
            spark = spark,
            schema = schema_name,
            table_name = table_name,
            database = data_settings.sql_database,
            server = data_settings.sql_server,
            user = sql_user,
            password = sql_pass,
            )
    except Exception as e:
        logger.warning(f'Error in reading table {schema_name}.{table_name}.')
        error_tables.append(f'{schema_name}.{table_name}')
        return

    if not table:
        error_tables.append(f'{schema_name}.{table_name}')
        return

    table = remove_column_spaces(table=table)

    key_column_names = get_key_column_names(table_name=file_meta['table_name'])
    table = add_id_key(table=table, key_column_names=key_column_names)

    dml_type = 'I' if file_meta['is_full_load'] else 'U'
    table = add_elt_columns(table=table, file_meta=file_meta, dml_type=dml_type)

    if is_pc: table.show(5)

    return {file_meta['table_name']: (table, key_column_names)}



# %% Translate Column Types

@catch_error(logger)
def get_dtypes(table, table_name:str):
    """
    Translate Column Types
    """
    dtypes = default_table_dtypes(table=table, use_varchar=True)
    return dtypes



# %% Iterate over all the files in all the firms and process them.

additional_file_meta_columns = []

migrate_all_files(
    spark = spark,
    fn_extract_file_meta = extract_sql_table_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_sql_table,
    fn_select_files = select_sql_tables,
    fn_get_dtypes = get_dtypes,
    )



# %%

if error_tables:
    logger.warning(f'Following {len(error_tables)} out of {len_table_list} tables were not migrated: {error_tables}')
else:
    logger.info(f'All {len_table_list} tables have been migrated')



# %% Close Connections / End Program

mark_execution_end()


# %%


