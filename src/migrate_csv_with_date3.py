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
        'source_path': r'C:\packages\Shared\METRICS_ASSETS\RAA'
        }



# %% Import Libraries

import os, sys

sys.args = args
sys.parent_name = os.path.basename(__file__)


from modules3.common_functions import ELT_PROCESS_ID_str, catch_error, data_settings, logger, mark_execution_end, is_pc
from modules3.spark_functions import add_md5_key, create_spark, read_csv, remove_column_spaces, add_elt_columns
from modules3.snowflake_ddl import connect_to_snowflake, snowflake_ddl_params
from modules3.migrate_files3 import migrate_all_files


from datetime import datetime

from pyspark.sql.functions import col, to_date



# %% Parameters




# %% Create Connections

spark = create_spark()
snowflake_ddl_params.spark = spark

snowflake_connection = connect_to_snowflake()
snowflake_ddl_params.snowflake_connection = snowflake_connection



# %% Extract Meta Data from csv file

@catch_error(logger)
def extract_csv_file_meta(file_path:str, cloud_file_history):
    """
    Extract Meta Data from csv file with date in file name
    """
    date_format = data_settings.date_format # http://strftime.org/

    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()

    if file_name_noext.count('_')>=2 and file_name_noext.upper().startswith(data_settings.pipeline_firm.upper()+'_'):
        file_name_noext = file_name_noext[file_name_noext.find('_')+1:]

    allowed_extensions = ['.txt', '.csv', '.zip']
    if file_ext.lower() not in allowed_extensions:
        logger.warning(f'Only {allowed_extensions} extensions are allowed: {file_path}')
        return

    if file_ext.lower() == '.zip':
        file_date_str = file_name_noext.split('_')[-1]
        table_name = ''
    else:
        date_loc = -file_name_noext[::-1].find('_')
        if date_loc>=0:
            logger.warning(f'Could not find date stamp for the file or invalid file name: {file_path}')
            return
        file_date_str = file_name_noext[date_loc:]
        table_name = file_name_noext[:date_loc-1]

    try:
        key_datetime = datetime.strptime(file_date_str, date_format)
        file_date = key_datetime.strftime(r'%Y-%m-%d')
    except:
        logger.warning(f'Invalid date format in file name: {file_path}')
        return

    file_meta = {
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'firm_name': data_settings.pipeline_firm,
        'file_date': file_date,
        'table_name': table_name.lower(),
        'is_full_load': data_settings.is_full_load.upper() == 'TRUE',
        'key_datetime': key_datetime,
        'pipelinekey': data_settings.pipelinekey,
        ELT_PROCESS_ID_str.lower(): data_settings.elt_process_id,
    }

    return file_meta



# %% Main Processing of sinlge csv File

@catch_error(logger)
def process_csv_file(file_meta, cloud_file_history):
    """
    Main Processing of single csv file
    """
    logger.info(file_meta)
    date_format = data_settings.date_format # http://strftime.org/

    file_path = os.path.join(file_meta['folder_path'], file_meta['file_name'])
    file_name_noext, file_ext = os.path.splitext(file_meta['file_name'])

    if file_name_noext.count('_')>=2 and file_name_noext.upper().startswith(data_settings.pipeline_firm.upper()+'_'):
        file_name_noext = file_name_noext[file_name_noext.find('_')+1:]

    if file_ext.lower() not in ['.txt', '.csv']:
        logger.warning(f'Not a .txt / .csv file: {file_path}')
        return

    date_loc = -file_name_noext[::-1].find('_')
    if date_loc>=0:
        logger.warning(f'Could not find date stamp for the file or invalid file name: {file_path}')
        return
    file_date_str = file_name_noext[date_loc:]

    try:
        key_datetime = datetime.strptime(file_date_str, date_format)
    except:
        logger.warning(f'Invalid date format in file name: {file_path}')
        return

    table_name =  file_name_noext[:date_loc-1].lower()

    table = read_csv(spark=spark, file_path=file_path)
    if not table: return

    table = remove_column_spaces(table=table)

    table = add_md5_key(table=table)

    table = add_elt_columns(
        table = table,
        key_datetime = key_datetime,
        is_full_load = file_meta['is_full_load'],
        dml_type = 'I' if file_meta['is_full_load'] else 'U',
        )

    if is_pc: table.show(5)
    if is_pc: print(f'Number of rows: {table.count()}')

    return {table_name: table}



# %% Iterate over all the files in all the firms and process them.

additional_ingest_columns = [
    to_date(col('file_date'), format='yyyy-MM-dd').alias('file_date', metadata={'sqltype': '[date] NULL'}),
    ]

migrate_all_files(
    spark = spark,
    snowflake_connection = snowflake_connection,
    fn_extract_file_meta = extract_csv_file_meta,
    additional_ingest_columns = additional_ingest_columns,
    fn_process_file = process_csv_file,
    )



# %% Close Connections

snowflake_connection.close()

mark_execution_end()



# %% Debugging








# %% Iterate over all the files in all the firms and process them.
"""

all_new_files, PARTITION_list, tableinfo = process_all_files_with_incrementals(
    spark = spark,
    firm_name = firm_name,
    data_path_folder = data_settings.source_path,
    fn_extract_file_meta = extract_csv_file_meta,
    date_start = date_start,
    additional_ingest_columns = additional_ingest_columns,
    fn_process_file = process_csv_file,
    key_column_names = key_column_names,
    tableinfo_source = tableinfo_source,
    date_column_name = date_column_name,
    )





# %% Save Tableinfo metadata table into Azure and Save Ingest files metadata to SQL Server.

tableinfo = save_tableinfo_dict_and_cloud_file_history(
    spark = spark,
    tableinfo = tableinfo,
    tableinfo_source = tableinfo_source,
    all_new_files = all_new_files,
    )



# %% Read metadata.TableInfo

table_rows = read_tableinfo_rows(tableinfo_name=tableinfo_name, tableinfo_source=tableinfo_source, tableinfo=tableinfo)



# %% Iterate Over Steps for all tables

ingest_data_list = iterate_over_all_tables_snowflake(
    tableinfo = tableinfo,
    table_rows = table_rows,
    firm_name = firm_name,
    PARTITION_list = PARTITION_list,
    )


# %% Create Source Level Tables

create_source_level_tables(ingest_data_list=ingest_data_list)


"""
