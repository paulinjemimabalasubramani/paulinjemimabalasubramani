"""
Generic Code to Migrate any CSV type files with date info in file name to ADLS Gen 2

"""


# %% Parse Arguments

if False: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description='Migrate any CSV type files with date info in file name')

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'METRICS_MIGRATE_ASSETS_RAA',
        'source_path': r'C:\myworkdir\Shared\METRICS_ASSETS\RAA'
        }



# %% Import Libraries

import os, sys

sys.args = args
sys.parent_name = os.path.basename(__file__)


from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc
from modules3.spark_functions import add_id_key, create_spark, read_csv, remove_column_spaces, add_elt_columns
from modules3.migrate_files import migrate_all_files, get_key_column_names


from datetime import datetime




# %% Parameters

allowed_file_extensions = ['.txt', '.csv']



# %% Create Connections

spark = create_spark()



# %% Extract Meta Data from csv file

@catch_error(logger)
def extract_csv_file_meta(file_path:str, zip_file_path:str=None):
    """
    Extract Meta Data from csv file with date in file name
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



# %% Main Processing of sinlge csv File

@catch_error(logger)
def process_csv_file(file_meta):
    """
    Main Processing of single csv file
    """
    table = read_csv(spark=spark, file_path=file_meta['file_path'])
    if not table: return

    table = remove_column_spaces(table=table)

    key_column_names = get_key_column_names(table_name=file_meta['table_name'])
    table = add_id_key(table=table, key_column_names=key_column_names)

    table = add_elt_columns(
        table = table,
        key_datetime = file_meta['key_datetime'],
        is_full_load = file_meta['is_full_load'],
        dml_type = 'I' if file_meta['is_full_load'] else 'U',
        )

    if is_pc: table.show(5)

    return {file_meta['table_name']: table}



# %% Iterate over all the files in all the firms and process them.

additional_file_meta_columns = [
    #('file_date', 'date NULL'),
    ]

migrate_all_files(
    spark = spark,
    fn_extract_file_meta = extract_csv_file_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_csv_file,
    )



# %% Close Connections / End Program

mark_execution_end()


# %%


