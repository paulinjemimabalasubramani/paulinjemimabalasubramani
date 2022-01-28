"""
Read all Albridge files and migrate to the ADLS Gen 2

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
        'pipelinekey': 'ASSETS_MIGRATE_ALBRIDGE_WFS',
        'source_path': r'C:\myworkdir\Shared\ALBRIDGE\WFS',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\assets\albridge_schema\albridge_schema.csv'
        }



# %% Import Libraries

import os, sys, csv
from datetime import datetime
from collections import defaultdict

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc
from modules3.spark_functions import add_id_key, create_spark, read_text, remove_column_spaces, add_elt_columns
from modules3.migrate_files import migrate_all_files, default_table_dtypes, file_meta_exists_for_select_files, add_firm_to_table_name

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit



# %% Parameters

allowed_file_extensions = ['.txt']

unused_column_name = 'unused'

master_schema_header_columns = {
    'file_date': 'datetime NULL',
    'eff_date': 'datetime NULL',
    'sequence_number': 'varchar(10) NULL',
    'fin_inst_id': 'varchar(10) NULL',
    }



# %% Create Connections

spark = create_spark()



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

            try:
                if file_ext.lower() == '.zip':
                    file_date_str = file_name_noext[-8:]
                else:
                    file_date_str = file_name_noext[-10:-2]

                key_datetime = datetime.strptime(file_date_str, data_settings.date_format)
                if key_datetime < data_settings.key_datetime: continue
            except:
                continue

            if file_meta_exists_for_select_files(file_path=file_path): continue

            selected_file_paths.append((file_path, key_datetime))

    selected_file_paths = sorted(selected_file_paths, key=lambda c: (c[1], c[0]))
    selected_file_paths = [c[0] for c in selected_file_paths]
    return file_count, selected_file_paths



# %% get and pre-process schema

@catch_error(logger)
def get_albridge_schema():
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    schema = defaultdict(list)
    albridge_file_types = dict()

    with open(data_settings.schema_file_path, newline='', encoding='utf-8-sig', errors='ignore') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            file_types = row['file_type'].upper().split(',')
            is_primary_key = row['is_primary_key'].strip().upper() == 'Y'

            column_name = row['column_name'].strip().lower()
            if column_name in ['', 'n/a', 'none', '_', '__', 'na', 'null', '-', '.']:
                column_name = unused_column_name

            file_type_desc = row['file_type_desc'].strip()

            for file_type in file_types:
                ftype = file_type.strip()
                schema[ftype].append({
                    'is_primary_key': is_primary_key,
                    'column_name': column_name,
                    })
                if ftype not in ['HEADER', 'TRAILER']:
                    albridge_file_types[ftype] = file_type_desc

    return schema, albridge_file_types



schema, albridge_file_types = get_albridge_schema()



# %% Extract Meta Data from Albridge file

@catch_error(logger)
def extract_albridge_file_meta(file_path:str, zip_file_path:str=None):
    """
    Extract Meta Data from Albridge file (reading 1st line (header metadata) from inside the file)
    """
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.upper()

    if file_ext.lower() not in allowed_file_extensions:
        logger.warning(f'Only {allowed_file_extensions} extensions are allowed: {file_path}')
        return

    if not 12<=len(file_name_noext)<=16 \
        or not file_name_noext[0].isalpha() \
        or not file_name_noext[2:].isdigit():
        logger.warning(f'Not a valid Albirdge Replication/Export file name: {file_path}')
        return

    if not (file_name_noext[:1] in albridge_file_types or file_name_noext[:2] in albridge_file_types):
        logger.warning(f'Unknown Albirdge Replication/Export file type: {file_path}')
        return

    sequence_number = file_name_noext[-2:]
    file_date = datetime.strptime(file_name_noext[-10:-2], data_settings.date_format)
    file_name_prefix = file_name_noext[:-10]

    fin_inst_id = data_settings.fin_inst_id
    if file_name_prefix[-len(fin_inst_id):].upper()!=fin_inst_id.upper():
        logger.warning(f'Financial Institution ID is incorrect. Found {file_name_prefix[-len(fin_inst_id):]} Expected: {fin_inst_id}')
        return

    file_type = file_name_prefix[:-len(fin_inst_id)].upper()
    if file_type not in albridge_file_types:
        logger.warning(f'Unknown Albridge file type: {file_type}')
        return

    with open(file=file_path, mode='rt', encoding='utf-8-sig', errors='ignore') as f:
        HEADER = f.readline()

    if HEADER[:2] != 'H|':
        logger.warning(f'Header is not found in 1st line inside the file: {file_path}')
        return

    HEADER = HEADER.split(sep='|')
    header_schema = [c['column_name'].lower() for c in schema['HEADER']]

    if len(HEADER)<len(header_schema):
        logger.warning(f'Invalid header length: {HEADER}')
        return

    header_meta = {header_schema[i]:HEADER[i].strip() for i in range(1, len(header_schema))}

    table_name = header_meta['file_desc'][:-4].lower()
    table_name = 'positions' if table_name == 'positionchanges' else table_name
    table_name = add_firm_to_table_name(table_name=table_name)

    key_datetime = datetime.strptime(' '.join([header_meta['run_date'], header_meta['run_time']]).strip(), r'%Y%m%d %H%M%S')

    file_meta = {
        'table_name': table_name.lower(), # table name should always be lower
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'zip_file_path': zip_file_path,
        'is_full_load':  file_type.upper() in ['R'], # Only Position files can be full load
        'key_datetime': key_datetime,

        'file_date': file_date,
        'sequence_number': sequence_number,
        'eff_date': datetime.strptime(header_meta['eff_date'].strip(), r'%Y%m%d'),
        'file_type': file_type,
        'file_type_desc': albridge_file_types[file_type],
        'fin_inst_id': fin_inst_id,
    }

    return file_meta



# %% Create table from given Albridge file and its schema

@catch_error(logger)
def create_table_from_albridge_file(file_meta:dict):
    file_schema = schema[file_meta['file_type']]
    text_file = read_text(spark=spark, file_path=file_meta['file_path'])

    text_file = (text_file
        .where(col('value').substr(0, 2)==lit('D|'))
        .withColumn('value', col('value').substr(lit(3), F.length(col('value'))))
        .withColumn('value', F.split(col('value'), '[|]'))
        .withColumnRenamed('value', 'elt_value')
        )

    key_column_names = []
    for i, sch in enumerate(file_schema):
        if sch['column_name'].lower() != unused_column_name:
            text_file = text_file.withColumn(sch['column_name'], col('elt_value').getItem(i))
            if sch['is_primary_key']:
                key_column_names.append(sch['column_name'])

    text_file = text_file.drop(col('elt_value'))

    table_columns = text_file.columns
    for column_name in master_schema_header_columns:
        if column_name not in table_columns:
            text_file = text_file.withColumn(column_name, lit(str(file_meta[column_name])))

    return text_file, key_column_names



# %% Main Processing of an Albridge File

@catch_error(logger)
def process_albridge_file(file_meta):
    """
    Main Processing of single Albridge file
    """

    table, key_column_names = create_table_from_albridge_file(file_meta=file_meta)
    if not table: return

    table = remove_column_spaces(table=table)
    table = add_id_key(table=table, key_column_names=key_column_names)

    table = add_elt_columns(
        table = table,
        key_datetime = file_meta['key_datetime'],
        is_full_load = file_meta['is_full_load'],
        dml_type = 'I' if file_meta['is_full_load'] else 'U',
        )

    if is_pc: table.show(5)

    return {file_meta['table_name']: table}



# %% Translate Column Types

@catch_error(logger)
def get_dtypes(table, table_name:str):
    """
    Translate Column Types
    """
    dtypes = default_table_dtypes(table=table, use_varchar=True)
    return dtypes



# %% Iterate over all the files in all the firms and process them.

additional_file_meta_columns = [(cname, ctype) for cname, ctype in master_schema_header_columns.items()]

migrate_all_files(
    spark = spark,
    fn_extract_file_meta = extract_albridge_file_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_albridge_file,
    fn_select_files = select_files,
    fn_get_dtypes = get_dtypes,
    )



# %% Close Connections / End Program

mark_execution_end()


# %%


