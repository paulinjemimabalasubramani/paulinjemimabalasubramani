"""
Read Pershing FWT files and migrate to the ADLS Gen 2

https://standardfiles.pershing.com/


This code assumes bulk_id has been added to the front of each line in the file.

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
        'pipelinekey': 'CA_MIGRATE_PERSHING_RAA',
        'source_path': r'C:\myworkdir\Shared\PERSHING\RAA',
        'bulk_path': r'C:\myworkdir\Shared\PERSHING\RAA_bulk',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\pershing_schema'
        }



# %% Import Libraries

import os, sys, hashlib, csv, re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc
from modules3.spark_functions import add_id_key, create_spark, remove_column_spaces, add_elt_columns, column_regex, read_text
from modules3.migrate_files import migrate_all_files, default_table_dtypes, add_firm_to_table_name

from collections import defaultdict
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit



# %% Parameters

master_schema_file = 'customer_acct_info' # to take header info
master_schema_form_name = 'customer_acct_info' # to take header info
master_schema_header_columns = {
    'date_of_data': 'datetime NULL',
    'remote_id': 'varchar(50) NULL',
    'refreshed_updated': 'varchar(10) NULL',
    }

allowed_file_extensions = ['.bulk']

hash_field_name = 'bulk_id'
hash_func = hashlib.sha1
total_hash_length = len(hash_func().hexdigest())
total_prefix_length = total_hash_length + 1

schema_header_str = 'HEADER'
schema_trailer_str = 'TRAILER'

bulk_id_header = 'HEADER'.ljust(total_hash_length)
bulk_id_trailer = 'TRAILER'.ljust(total_hash_length)

master_groupBy = [hash_field_name]

pershing_strftime = r'%m/%d/%Y %H:%M:%S' # http://strftime.org/

data_settings.source_path = data_settings.bulk_path # Use files from bulk path

table_special_records = defaultdict(dict)



# %% Create Connections

spark = create_spark()



# %% Select Files

@catch_error(logger)
def select_files():
    """
    Initial Selection of candidate files potentially to be ingested
    """
    source_path = data_settings.source_path
    selected_file_paths = []

    for root, dirs, files in os.walk(source_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() not in allowed_file_extensions + ['.zip']: continue

            selected_file_paths.append(file_path)

    selected_file_paths = sorted(selected_file_paths)
    return selected_file_paths



# %% get and pre-process schema

@catch_error(logger)
def get_pershing_schema(schema_file_name:str):
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    csv_encoding = 'utf-8-sig'

    if not schema_file_name.endswith('.csv'): schema_file_name = schema_file_name + '.csv'
    schema_file_path = os.path.join(data_settings.schema_file_path, schema_file_name)
    if not os.path.isfile(schema_file_path):
        logger.warning(f'Schema file is not found: {schema_file_path}')
        return

    schema = []
    record_names = []
    with open(schema_file_path, mode='rt', newline='', encoding=csv_encoding, errors='ignore') as fs:
        reader = csv.DictReader(fs)
        for row in reader:
            rowl = {k.lower().strip():v.strip() for k, v in row.items()}
            field_name = re.sub(column_regex, '_', rowl['field_name'].lower().strip())
            position = rowl['position'].strip()
            record_name = rowl['record_name'].upper().strip()
            conditional_changes = rowl['conditional_changes'].upper().strip()

            if not field_name or (field_name in ['', 'not_used', '_', '__', 'n_a', 'na', 'none', 'null', 'value']) \
                or ('-' not in position) or not record_name: continue

            if record_name not in record_names:
                record_names.append(record_name)
                schema.append({
                    'field_name': hash_field_name.lower(),
                    'position_start': 1,
                    'position_end': total_hash_length,
                    'length': total_hash_length,
                    'record_name': record_name,
                    'conditional_changes': conditional_changes,
                    })

            pos = position.split('-')
            position_start = int(pos[0]) + total_prefix_length
            position_end = int(pos[1]) + total_prefix_length
            if position_start > position_end:
                raise ValueError(f'position_start {pos[0]} > position_end {pos[1]} for field_name {field_name} in record_name {record_name}')

            schema.append({
                'field_name': field_name,
                'position_start': position_start,
                'position_end': position_end,
                'length': position_end - position_start + 1,
                'record_name': record_name,
                'conditional_changes': conditional_changes,
                })

    return schema



# %% Get Header Schema from Master Pershing Schema

@catch_error(logger)
def get_header_schema():
    """
    Get Header Schema from Master Pershing Schema
    """
    position_fields = ['position_start', 'position_end', 'length']

    schema = get_pershing_schema(schema_file_name=master_schema_file)
    if not schema: raise Exception(f'Main Schema file "{master_schema_file}" is not found!')

    header_schema = [c for c in schema if c['record_name'] == schema_header_str]
    header_schema = {r['field_name']: {field_name: r[field_name] for field_name in position_fields} for r in header_schema}
    header_schema['form_name'] = header_schema.pop(master_schema_form_name)
    return header_schema



header_schema = get_header_schema()



# %% Get Header Info from Pershing file

@catch_error(logger)
def get_header_info(file_path:str):
    """
    Get Header Info from Pershing file
    """
    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    if HEADER[:len(bulk_id_header)] != bulk_id_header: 
        logger.warning(f'Not a Bulk Formatted file: {file_path}')
        return

    header_info = dict()
    for field_name, pos in header_schema.items():
        header_info[field_name] = re.sub(' +', ' ', HEADER[pos['position_start']-1: pos['position_end']].strip())

    header_info['date_of_data'] = datetime.strptime(header_info['date_of_data'], r'%m/%d/%Y')
    return header_info



# %% Extract field from a fixed width table

@catch_error(logger)
def extract_field_from_fwt(table, field_name:str, position_start:int, length:int):
    """
    Extract a field from a fixed width table
    """
    cv = col('value').substr
    return table.withColumn(field_name, F.regexp_replace(F.trim(cv(position_start, length)), ' +', ' '))



# %% Add Schema fields to table

@catch_error(logger)
def add_schema_fields_to_table(tables, table, schema, record_name:str, conditional_changes:str=''):
    """
    Add fileds defined in Schema to the table and collect in "tables" list bases on record_name and conditional_changes
    """
    record_name = record_name.upper()

    schema = [c for c in schema if c['record_name'] == record_name]
    if not schema: return

    for schema_dict in schema:
        table = extract_field_from_fwt(table=table, field_name=schema_dict['field_name'], position_start=schema_dict['position_start'], length=schema_dict['length'])

    table = table.drop('value')
    tables[(record_name, conditional_changes)] = table

    if is_pc:
        print(f'\n\nrecord_name: {record_name}, conditional_changes: {conditional_changes}\n')
        if False: table.show(5)
        print('\n')



# %% Add Sub-tables to table

@catch_error(logger)
def add_sub_tables_to_table(tables, schema, sub_tables, record_name:str):
    """
    Add sub-tables from special_records (with conditional_changes) to the tables list
    """
    for conditional_changes, sub_table in sub_tables.items():
        filter_schema = [c for c in schema if (c['record_name'] == record_name) and \
            ((conditional_changes and conditional_changes.upper() in c['conditional_changes']) or (not c['conditional_changes']))]

        add_schema_fields_to_table(tables=tables, table=sub_table, schema=filter_schema, record_name=record_name, conditional_changes=conditional_changes)



# %% generate tables from fixed width table file

@catch_error(logger)
def generate_tables_from_fwt(
        text_file,
        schema,
        table_name:str,
        ):
    """
    Generate list of sub-tables from a fixed width table file based on record_name and conditional_changes
    """
    tables = dict()
    cv = col('value').substr

    special_records = table_special_records[table_name]
    record_names = sorted(list({c['record_name'] for c in schema}))

    for record_name in record_names:
        if is_pc: print(f'Record Name: {record_name}\n')
        if record_name.upper() in [schema_header_str, schema_trailer_str]: continue

        table = text_file.where((cv(3+total_prefix_length, 1)==lit(record_name)) & (cv(1, len(bulk_id_header))!=lit(bulk_id_header)) & (cv(1, len(bulk_id_trailer))!=lit(bulk_id_trailer)))

        if is_pc:
            if False: table.show(5, False)
            print('\n')

        if record_name in special_records:
            sub_tables = special_records[record_name](table=table, schema=schema, record_name=record_name)
            add_sub_tables_to_table(tables=tables, schema=schema, sub_tables=sub_tables, record_name=record_name)
        else:
            add_schema_fields_to_table(tables=tables, table=table, schema=schema, record_name=record_name)

    return tables



# %% Union Tables per Record

@catch_error(logger)
def union_tables_per_record(tables):
    """
    Union Tables per Record (union conditional_changes)
    """
    table_per_record = dict()
    for key, table in tables.items():
        record_name = key[0]
        if record_name in table_per_record:
            table_per_record[record_name] = table_per_record[record_name].unionByName(table, allowMissingColumns=True)
        else:
            table_per_record[record_name] = table

    return table_per_record



# %% Combine Rows into Array

@catch_error(logger)
def combine_rows_into_array(tables):
    """
    Utility function to combine rows into Array (so that to have one column of json data per record name in the final main table)
    """
    for record_name in tables.keys():
        if record_name not in [schema_header_str, schema_trailer_str]:
            tables[record_name] = (tables[record_name]
                .withColumn('all_columns', F.struct(tables[record_name].columns))
                .groupBy(master_groupBy)
                .agg(F.collect_list('all_columns').alias(f'record_{record_name}'))
                )

    return tables



# %% Join All Tables

@catch_error(logger)
def join_all_tables(tables, file_meta:dict):
    """
    Join all sub-tables that were split by record_name and conditional_changes
    """
    joined_tables = None

    for record_name, table in tables.items():
        if record_name not in [schema_header_str, schema_trailer_str]:
            if joined_tables:
                joined_tables = joined_tables.join(table, on=master_groupBy, how='full')
            else:
                joined_tables = table

    table_columns = joined_tables.columns
    for column_name in master_schema_header_columns:
        if column_name not in table_columns:
            joined_tables = joined_tables.withColumn(column_name, lit(str(file_meta[column_name])))

    return joined_tables



# %% Create Table from Fixed-With Table File

@catch_error(logger)
def create_table_from_fwt_file(file_meta:dict):
    """
    Create Table from Fixed-With Table File. Convery FWT to nested Spark table.
    """
    data_file_path = file_meta['file_path']
    table_name = file_meta['table_name']
    schema_file_name = file_meta['schema_file_name']

    logger.info({
        'action': 'generate_tables_from_fwt_file',
        'data_file_path': data_file_path,
        'schema_file_name': schema_file_name,
    })

    schema = get_pershing_schema(schema_file_name=schema_file_name)
    if not schema: return

    text_file = read_text(spark=spark, file_path=data_file_path)

    tables = generate_tables_from_fwt(text_file=text_file, schema=schema, table_name=table_name)
    tables = union_tables_per_record(tables=tables)
    tables = combine_rows_into_array(tables=tables)

    table = join_all_tables(tables=tables, file_meta=file_meta)
    return table



# %% Process customer_acct_info Record A

@catch_error(logger)
def process_record_A_customer_acct_info(table, schema, record_name):
    """
    Special Record processing for customer_acct_info Record A - because of it has conditional_changes
    """
    if record_name != 'A': return

    field_name = 'registration_type'
    field_pos = [c for c in schema if c['record_name'] == record_name and c['field_name'] == field_name][0]
    table = extract_field_from_fwt(table=table, field_name=field_name, position_start=field_pos['position_start'], length=field_pos['length'])

    registration_types = ['JNTN', 'TODJ', 'CUST', '529C', 'TRST', '529T', 'NPLT', 'CPPS']

    sub_tables = {registration_type:table.where(col(field_name)==lit(registration_type)) for registration_type in registration_types}
    sub_tables = {**sub_tables, '':table.where(~col(field_name).isin(registration_types))}
    return sub_tables


table_special_records['customer_acct_info']['A'] = process_record_A_customer_acct_info



# %% Process customer_acct_info Record C

@catch_error(logger)
def process_record_C_customer_acct_info(table, schema, record_name):
    """
    Special Record processing for customer_acct_info Record C - because of it has conditional_changes
    """
    if record_name != 'C': return

    field_name = 'country_code_1'
    field_pos = [c for c in schema if c['record_name'] == record_name and c['field_name'] == field_name][0]
    table = extract_field_from_fwt(table=table, field_name=field_name, position_start=field_pos['position_start'], length=field_pos['length'])

    USCA_codes = ['US','CA']

    sub_tables = {
        'US or Canada addresses only': table.where(col(field_name).isin(USCA_codes)),
        'Non-US or Canada addresses only': table.where(~col(field_name).isin(USCA_codes)),
    }
    return sub_tables


table_special_records['customer_acct_info']['C'] = process_record_C_customer_acct_info



# %% Extract Meta Data from Pershing FWT file

@catch_error(logger)
def extract_pershing_file_meta(file_path:str, zip_file_path:str=None):
    """
    Extract Meta Data from Pershing FWT file (reading 1st line (header metadata) from inside the file)
    """
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()

    if file_ext.lower() not in allowed_file_extensions:
        logger.warning(f'Only {allowed_file_extensions} extensions are allowed: {file_path}')
        return

    header_info = get_header_info(file_path=file_path)

    schema_file_name = re.sub(' ', '_', header_info['form_name'].lower())
    table_name = add_firm_to_table_name(table_name=schema_file_name)

    file_meta = {
        'table_name': table_name,
        'schema_file_name': schema_file_name,
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'zip_file_path': zip_file_path,
        'is_full_load': data_settings.is_full_load.upper() == 'TRUE',
        'key_datetime': datetime.strptime(' '.join([header_info['run_date'], header_info['run_time']]), pershing_strftime),
        **{c:header_info[c] for c in master_schema_header_columns},
    }

    return file_meta



# %% Main Processing of Pershing File

@catch_error(logger)
def process_pershing_file(file_meta):
    """
    Main Processing of single Pershing file
    """
    table = create_table_from_fwt_file(file_meta=file_meta)
    if not table: return

    table = remove_column_spaces(table=table)

    table = add_id_key(table=table, key_column_names=master_groupBy)

    table = add_elt_columns(
        table = table,
        key_datetime = file_meta['key_datetime'],
        is_full_load = file_meta['is_full_load'],
        dml_type = 'I' if file_meta['is_full_load'] else 'U',
        )

    if is_pc and False: table.show(5)

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
additional_file_meta_columns.append(('schema_file_name', 'varchar(250) NULL'))

migrate_all_files(
    spark = spark,
    fn_extract_file_meta = extract_pershing_file_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_pershing_file,
    fn_select_files = select_files,
    fn_get_dtypes = get_dtypes,
    )



# %% Close Connections / End Program

mark_execution_end()


# %%


