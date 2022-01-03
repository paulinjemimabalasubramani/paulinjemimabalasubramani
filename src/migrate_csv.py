"""
Generic Code to Migrate any CSV type files with date info in file name to ADLS Gen 2

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description='Migrate any CSV type files with date info in file name')

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_PRODUCT_AG',
        'source_path': r'C:\packages\Shared\AG_ASSETS_PRODUCT'
        }



# %% Import Libraries

import os, sys

sys.args = args
sys.parent_name = os.path.basename(__file__)

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules2.common_functions import EXECUTION_DATE_str, catch_error, data_settings, logger, mark_execution_end, is_pc, get_pipeline_info, \
    execution_date_start, execution_date
from modules2.spark_functions import IDKeyIndicator, add_md5_key, create_spark, read_csv, remove_column_spaces, add_elt_columns
from modules2.azure_functions import read_tableinfo_rows, tableinfo_name
from modules2.snowflake_ddl import connect_to_snowflake, iterate_over_all_tables_snowflake, create_source_level_tables, snowflake_ddl_params
from modules2.migrate_files import save_tableinfo_dict_and_cloud_file_history, process_all_files_with_incrementals, get_key_column_names

from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql.functions import col, lit, to_date, to_timestamp



# %% Parameters

tableinfo_source = data_settings.schema_name
date_start = datetime.strptime(data_settings.file_history_start_date, r'%Y-%m-%d')

date_column_name = 'run_datetime'
key_column_names = get_key_column_names(date_column_name=date_column_name)

pipeline_info = get_pipeline_info(pipelinekey=data_settings.pipelinekey)
firm_name = pipeline_info['firm'].upper()

logger.info({
    'schema_name': data_settings.schema_name,
    'source_path': data_settings.source_path,
    **pipeline_info,
})



# %% Create Spark Session

spark = create_spark()
snowflake_ddl_params.spark = spark


# %% Extract Meta Data from csv file

@catch_error(logger)
def extract_csv_file_meta(file_path:str, cloud_file_history):
    """
    Extract Meta Data from csv file with date in file name
    """
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()

    allowed_extensions = ['.txt', '.csv', '.zip']
    if file_ext.lower() not in allowed_extensions:
        logger.warning(f'Only {allowed_extensions} extensions are allowed: {file_path}')
        return

    if file_ext.lower() == '.zip':
        table_name = ''
    else:
        table_name = file_name_noext

    file_meta = {
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'firm_name': firm_name,
        'table_name': table_name.lower(),
        'is_full_load': data_settings.is_full_load.upper() == 'TRUE',
        date_column_name: execution_date_start,
    }

    return file_meta



# %% Main Processing of sinlge csv File

@catch_error(logger)
def process_csv_file(file_meta, cloud_file_history):
    """
    Main Processing of single csv file
    """
    logger.info(file_meta)

    file_path = os.path.join(file_meta['folder_path'], file_meta['file_name'])
    file_name_noext, file_ext = os.path.splitext(file_meta['file_name'])

    if file_ext.lower() not in ['.txt', '.csv']:
        logger.warning(f'Not a .txt / .csv file: {file_path}')
        return

    table_name =  file_name_noext.lower()

    table = read_csv(spark=spark, file_path=file_path)
    if not table: return

    if not IDKeyIndicator.upper() in [c.upper() for c in table.columns]:
        table = add_md5_key(table=table)

    table = remove_column_spaces(table=table)
    table = table.withColumn(date_column_name, lit(execution_date))
    table = table.withColumn('firm_name', lit(str(firm_name)))

    table = add_elt_columns(
        table = table,
        reception_date = execution_date_start,
        source = tableinfo_source,
        is_full_load = file_meta['is_full_load'],
        dml_type = 'I' if file_meta['is_full_load'] else 'U',
        )

    #table.persist(StorageLevel.MEMORY_AND_DISK)

    if is_pc: table.show(5)
    if is_pc: print(f'Number of rows: {table.count()}')

    return {table_name: table}



# %% Iterate over all the files in all the firms and process them.

additional_ingest_columns = [
    to_timestamp(col(date_column_name)).alias(date_column_name, metadata={'sqltype': '[datetime] NULL'}),
    ]

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


# %% Connect to SnowFlake

snowflake_connection = connect_to_snowflake()
snowflake_ddl_params.snowflake_connection = snowflake_connection


# %% Iterate Over Steps for all tables

ingest_data_list = iterate_over_all_tables_snowflake(
    tableinfo = tableinfo,
    table_rows = table_rows,
    firm_name = firm_name,
    PARTITION_list = PARTITION_list,
    )


# %% Create Source Level Tables

create_source_level_tables(ingest_data_list=ingest_data_list)


# %% Close Showflake connection

snowflake_connection.close()


# %% Mark Execution End

mark_execution_end()


# %%

