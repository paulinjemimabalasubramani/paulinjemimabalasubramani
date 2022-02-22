description = """

Read National Financial Services (NFS) FWT files and migrate to the ADLS Gen 2

This code uses NFS files converted to JSON format

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
        'pipelinekey': 'CA_MIGRATE_NFS_RAA',
        }



# %% Import Libraries

import os, sys
from datetime import datetime

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc
from modules3.spark_functions import add_id_key, create_spark, read_json, remove_column_spaces, add_elt_columns
from modules3.migrate_files import migrate_all_files, get_key_column_names, default_table_dtypes, file_meta_exists_for_select_files, add_firm_to_table_name



# %% Parameters

allowed_file_extensions = ['.json']

data_settings.date_format = r'%Y%m%d'

data_settings.source_path = data_settings.app_data_path # Use files from app_data_path



# %% Create Connections

spark = create_spark()



# %% Select Files

@catch_error(logger)
def select_files():
    """
    Initial Selection of candidate files potentially to be ingested
    """
    date_format = data_settings.date_format # http://strftime.org/
    selected_file_paths = []

    file_count = 0
    for root, dirs, files in os.walk(data_settings.source_path):
        for file_name in files:
            file_count += 1
            file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() not in allowed_file_extensions: continue

            try:
                date_loc = -file_name_noext[::-1].find('_')
                file_date_str = file_name_noext[date_loc:]

                key_datetime = datetime.strptime(file_date_str, date_format)
                if key_datetime < data_settings.key_datetime: continue
            except:
                logger.warning(f'Invalid Date Format: {file_path}')
                continue

            if file_meta_exists_for_select_files(file_path=file_path): continue

            selected_file_paths.append((file_path, key_datetime))

    selected_file_paths = sorted(selected_file_paths, key=lambda c: (c[1], c[0]))
    selected_file_paths = [c[0] for c in selected_file_paths]
    return file_count, selected_file_paths



# %% Extract Meta Data from NA json file

@catch_error(logger)
def extract_na_json_file_meta(file_path:str, zip_file_path:str=None):
    """
    Extract Meta Data from NA json file with date in file name
    """
    date_format = data_settings.date_format # http://strftime.org/

    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()

    if file_ext.lower() not in allowed_file_extensions:
        logger.warning(f'Only {allowed_file_extensions} extensions are allowed: {file_path}')
        return

    date_loc = -file_name_noext[::-1].find('_')
    if date_loc>=0:
        logger.warning(f'Could not find date stamp for the file or invalid file name: {file_path}')
        return
    file_date_str = file_name_noext[date_loc:]
    table_name = file_name_noext[:date_loc-1]
    table_name = add_firm_to_table_name(table_name=table_name)

    try:
        key_datetime = datetime.strptime(file_date_str, date_format)
    except:
        logger.warning(f'Invalid date format in file name: {file_path}')
        return

    file_meta = {
        'table_name': table_name.lower(), # table name should always be lower
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'zip_file_path': zip_file_path,
        'is_full_load': data_settings.is_full_load.upper() == 'TRUE',
        'key_datetime': key_datetime,
    }

    return file_meta



# %% Main Processing of sinlge NA json File

@catch_error(logger)
def process_na_json_file(file_meta):
    """
    Main Processing of single NA json file
    """
    table = read_json(spark=spark, file_path=file_meta['file_path'])
    if not table: return

    table = remove_column_spaces(table=table)

    firm_name = file_meta['table_name'].split('_')[0].upper()
    key_column_names = get_key_column_names(table_name=file_meta['table_name'], firm_name=firm_name)
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
    fn_extract_file_meta = extract_na_json_file_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_na_json_file,
    fn_select_files = select_files,
    fn_get_dtypes = get_dtypes,
    )



# %% Close Connections / End Program

mark_execution_end()


# %%


