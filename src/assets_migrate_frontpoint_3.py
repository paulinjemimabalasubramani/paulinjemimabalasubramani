description = """

Read all ASSETS - FRONTPOINT files and migrate to the ADLS Gen 2

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
        'pipelinekey': 'ASSETS_MIGRATE_FRONTPOINT',
        'source_path': r'C:\myworkdir\Shared\FRONTPOINT',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\assets\frontpoint_schema\frontpoint_schema.csv',
        }



# %% Import Libraries

import os, sys, csv
from datetime import datetime
from collections import defaultdict

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, execution_date_start
from modules3.spark_functions import add_id_key, create_spark, remove_column_spaces, add_elt_columns, read_text
from modules3.migrate_files import migrate_all_files, default_table_dtypes, file_meta_exists_for_select_files, add_firm_to_table_name

from pyspark.sql.functions import col
import pyspark.sql.functions as F



# %% Parameters

allowed_file_extensions = ['.gz']

unused_column_name = 'unused'
data_separator = '#!#!'



# %% Create Connections

spark = create_spark()



# %% get and pre-process schema


@catch_error(logger)
def get_frontpoint_schema():
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    schema_file_path = data_settings.schema_file_path

    file_schema = defaultdict(list)

    with open(schema_file_path, newline='', encoding='utf-8-sig', errors='ignore') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            table_name = row['table_name'].strip().lower()
            is_primary_key = row['is_primary_key'].strip().upper() == 'Y'

            column_name = row['column_name'].strip().lower()
            if column_name in ['', 'n/a', 'none', '_', '__', 'na', 'null', '-', '.']:
                column_name = unused_column_name

            file_schema[table_name].append({
                'is_primary_key': is_primary_key,
                'column_name': column_name,
                })

    return file_schema



file_schema = get_frontpoint_schema()



# %% Select Files

@catch_error(logger)
def select_files():
    """
    Initial Selection of candidate files potentially to be ingested
    """
    selected_file_paths = []

    file_count = 0
    for root, dirs, files in os.walk(data_settings.source_path):
        for file_name in files:
            file_count += 1
            file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() not in allowed_file_extensions + ['.zip']: continue

            if file_ext.lower() == '.zip':
                key_datetime = execution_date_start
            else:
                file_name_list = file_name_noext.split('_')
                if not len(file_name_list) == 6: continue

                year, month, day = file_name_list[2], file_name_list[3], file_name_list[4]
                try:
                    key_datetime = datetime.strptime('-'.join([year, month, day]), r'%Y-%m-%d')
                except Exception as e:
                    logger.warning(f'Invalid Date Format: {file_path}. {str(e)}')
                    continue

            if file_meta_exists_for_select_files(file_path=file_path): continue

            selected_file_paths.append((file_path, key_datetime))

    selected_file_paths = sorted(selected_file_paths, key=lambda c: (c[1], c[0]))
    selected_file_paths = [c[0] for c in selected_file_paths]
    return file_count, selected_file_paths



# %% Extract Meta Data from Frontpoint file

@catch_error(logger)
def extract_frontpoint_file_meta(file_path:str, zip_file_path:str=None):
    """
    Extract Meta Data from Frontpoint file name
    """
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()
    file_name_list = file_name_noext.split('_')

    if file_ext.lower() not in allowed_file_extensions:
        logger.warning(f'Only {allowed_file_extensions} extensions are allowed: {file_path}')
        return

    if not len(file_name_list) == 6:
        logger.warning(f'Cannot parse Frontpoint file name: {file_path}')
        return

    table_name = "_".join([file_name_list[0], file_name_list[1]]).lower()
    if table_name not in file_schema:
        logger.warning(f'Table name should be one of {list(file_schema)} for file: {file_path}')
        return

    table_name = add_firm_to_table_name(table_name=table_name)

    year, month, day = file_name_list[2], file_name_list[3], file_name_list[4]
    try:
        key_datetime = datetime.strptime('-'.join([year, month, day]), r'%Y-%m-%d')
    except Exception as e:
        logger.warning(f'Cannot parse datetime for file: {file_path}. {str(e)}')
        return

    sequence_number = file_name_list[5].lower()
    is_full_load = sequence_number == 'full'

    file_meta = {
        'table_name': table_name.lower(), # table name should always be lower
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'zip_file_path': zip_file_path,
        'is_full_load': is_full_load,
        'key_datetime': key_datetime,
        'sequence_number': sequence_number,
    }

    return file_meta



# %% Create table from given Frontpoint file and its schema

@catch_error(logger)
def create_table_from_frontpoint_file(file_path:str, file_schema):
    text_file = read_text(spark=spark, file_path=file_path)
    if not text_file: return

    text_file = (text_file
        .withColumn('value', F.split(col('value'), data_separator))
        .withColumnRenamed('value', 'elt_value')
        )

    for i, sch in enumerate(file_schema):
        if sch['column_name'].lower() != unused_column_name:
            text_file = text_file.withColumn(sch['column_name'], col('elt_value').getItem(i))

    text_file = text_file.drop(col('elt_value'))
    return text_file



# %% Get Key Column Names for a Frontpoint table

@catch_error(logger)
def get_key_column_names_frontpoint(table_name):
    """
    Get Key Column Names for a Frontpoint table
    """
    key_column_names = [c['column_name'].lower() for c in file_schema[table_name] if c['is_primary_key']]
    return key_column_names



# %% Main Processing of single Frontpoint File

@catch_error(logger)
def process_frontpoint_file(file_meta):
    """
    Main Processing of single Frontpoint file
    """
    table = create_table_from_frontpoint_file(
        file_path = file_meta['file_path'],
        file_schema = file_schema[file_meta['table_name']],
        )
    if not table: return

    table = remove_column_spaces(table=table)

    key_column_names = get_key_column_names_frontpoint(table_name=file_meta['table_name'])
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

additional_file_meta_columns = [
    ('sequence_number', 'varchar(10) NULL'),
    ]

migrate_all_files(
    spark = spark,
    fn_extract_file_meta = extract_frontpoint_file_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_frontpoint_file,
    fn_select_files = select_files,
    fn_get_dtypes = get_dtypes,
    )



# %% Close Connections / End Program

mark_execution_end()


# %%


