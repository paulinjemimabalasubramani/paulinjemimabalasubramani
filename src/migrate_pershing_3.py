description = """

Read Pershing FWT files and migrate to the ADLS Gen 2

https://standardfiles.pershing.com/


This code assumes bulk_id has been added to the front of each line in the file.

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
        'pipelinekey': 'ASSETS_MIGRATE_PERSHING_RAA',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\pershing_schema',
        }



# %% Import Libraries

import os, sys, re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, column_regex
from modules3.spark_functions import add_id_key, create_spark, remove_column_spaces, add_elt_columns, read_text
from modules3.migrate_files import migrate_all_files, default_table_dtypes, add_firm_to_table_name, get_key_column_names
from modules3.pershing_header import bulk_file_ext, schema_header_str, schema_trailer_str, total_prefix_length, bulk_id_header, \
    bulk_id_trailer, master_schema_header_columns, get_pershing_schema, get_header_info, hash_field_name

from collections import defaultdict
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType



# %% Parameters

allowed_file_extensions = [bulk_file_ext]

start_line_pos_start = int(data_settings.start_line_record_position) + total_prefix_length

pershing_strftime = r'%m/%d/%Y %H:%M:%S' # http://strftime.org/

data_settings.source_path = data_settings.app_data_path # Use files from app_data_path

table_special_records = defaultdict(dict)



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

            selected_file_paths.append(file_path)

    selected_file_paths = sorted(selected_file_paths)
    return file_count, selected_file_paths


# %% Add Scale

@udf(StringType())
def add_scale(field_value:str, scale:int=-1):
    """
    Utility PySpark UDF for adding Scale to Numbers if exists
    """
    if not field_value or scale < 0 or not field_value.isdigit(): return field_value

    if scale == 0:
        field_value = int(field_value)
    else:
        x = len(field_value) - scale
        field_value = float(field_value[:x] + '.' + field_value[x:])

    field_value = str(field_value)
    return field_value



# %% Extract field from a fixed width table

@catch_error(logger)
def extract_field_from_fwt(table, field_name:str, position_start:int, length:int, scale:int=-1):
    """
    Extract a field from a fixed width table
    """
    cv = col('value').substr
    table = table.withColumn(field_name, F.regexp_replace(F.trim(cv(position_start, length)), ' +', ' '))

    if scale >= 0:
        table = table.withColumn(field_name, add_scale(col(field_name), lit(scale)))

    return table



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
        table = extract_field_from_fwt(table=table, field_name=schema_dict['field_name'], position_start=schema_dict['position_start'], length=schema_dict['length'], scale=schema_dict['scale'])

    table = table.drop('value')
    tables[(record_name, conditional_changes)] = table

    if is_pc:
        print(f'\n\nSchema: {schema}')
        print(f'\n\nadd_schema_fields_to_table record_name: {record_name}, conditional_changes: {conditional_changes}\n')
        if True: table.show(5)
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
    if table_name.lower().endswith('expanded_sec_desc'):
        start_ = start_line_pos_start - 2
    else: 
        start_ = start_line_pos_start

    tables = dict()
    cv = col('value').substr

    special_records = table_special_records[table_name]
    record_names = sorted(list({c['record_name'] for c in schema}))

    for record_name in record_names:
        if is_pc: print(f'Record Name: {record_name}\n')
        if record_name.upper() in [schema_header_str, schema_trailer_str]: continue

        table = text_file.where((cv(start_, len(record_name))==lit(record_name)) & (cv(1, len(bulk_id_header))!=lit(bulk_id_header)) & (cv(1, len(bulk_id_trailer))!=lit(bulk_id_trailer)))

        if is_pc:
            if True: table.show(5, False)
            print('\n')

        if record_name in special_records:
            sub_tables = special_records[record_name](table=table, schema=schema, record_name=record_name)
            add_sub_tables_to_table(tables=tables, schema=schema, sub_tables=sub_tables, record_name=record_name)
        else:
            add_schema_fields_to_table(tables=tables, table=table, schema=schema, record_name=record_name)

    if is_pc:
        print('\n\ngenerate_tables_from_fwt')
        for (record_name, conditional_changes), table in tables.items():
            print(f'\n\n\nrecord_name: {record_name}, conditional_changes: {conditional_changes}\n')
            table.show(5)

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

    if is_pc:
        print('\n\nunion_tables_per_record')
        for record_name, table in tables.items():
            print(f'\n\n\nrecord_name: {record_name}\n')
            table.show(5)

    return table_per_record



# %% Combine Rows into Array

@catch_error(logger)
def combine_rows_into_array(tables, key_column_names:list):
    """
    Utility function to combine rows into Array (so that to have one column of json data per record name in the final main table)
    """
    for record_name in tables.keys():
        if record_name not in [schema_header_str, schema_trailer_str]:
            tables[record_name] = (tables[record_name]
                .withColumn('all_columns', F.struct(tables[record_name].columns))
                .groupBy(key_column_names)
                .agg(F.collect_list('all_columns').alias(f'record_{record_name}'))
                )

    if is_pc:
        print('\n\ncombine_rows_into_array')
        for record_name, table in tables.items():
            print(f'\n\n\nrecord_name: {record_name}\n')
            table.show(5)

    return tables



# %% Join All Tables

@catch_error(logger)
def join_all_tables(tables, file_meta:dict, key_column_names:list):
    """
    Join all sub-tables that were split by record_name and conditional_changes
    """
    joined_tables = None

    for record_name, table in tables.items():
        if record_name not in [schema_header_str, schema_trailer_str]:
            if joined_tables:
                joined_tables = joined_tables.join(table, on=key_column_names, how='full')
            else:
                joined_tables = table

    table_columns = joined_tables.columns
    for column_name in master_schema_header_columns:
        if column_name not in table_columns:
            joined_tables = joined_tables.withColumn(column_name, lit(str(file_meta[column_name])))

    return joined_tables



# %% Create Table from Fixed-With Table File

@catch_error(logger)
def create_table_from_fwt_file(file_meta:dict, key_column_names:list):
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

    schema = get_pershing_schema(schema_file_name=schema_file_name, table_name=table_name)
    if not schema: return

    text_file = read_text(spark=spark, file_path=data_file_path)
    if not text_file: return

    if is_pc:
        text_file.show(5)

    tables = generate_tables_from_fwt(text_file=text_file, schema=schema, table_name=table_name)
    tables = union_tables_per_record(tables=tables)
    tables = combine_rows_into_array(tables=tables, key_column_names=key_column_names)

    table = join_all_tables(tables=tables, file_meta=file_meta, key_column_names=key_column_names)
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
    table = extract_field_from_fwt(table=table, field_name=field_name, position_start=field_pos['position_start'], length=field_pos['length'], scale=field_pos['scale'])

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
    table = extract_field_from_fwt(table=table, field_name=field_name, position_start=field_pos['position_start'], length=field_pos['length'], scale=field_pos['scale'])

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

    schema_file_name = re.sub(column_regex, ' ', header_info['form_name'].lower()).strip()
    schema_file_name = re.sub(' ', '_', schema_file_name)
    table_name = add_firm_to_table_name(table_name=schema_file_name)
    if schema_file_name.startswith('security_profile_'):
        schema_file_name = 'security_profiles'

    is_full_load = header_info['refreshed_updated'].upper() == 'REFRESHED'

    file_meta = {
        'table_name': table_name,
        'schema_file_name': schema_file_name,
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'zip_file_path': zip_file_path,
        'is_full_load': is_full_load,
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
    key_column_names = get_key_column_names(table_name=file_meta['table_name'])
    if not key_column_names: key_column_names = [hash_field_name]

    table = create_table_from_fwt_file(file_meta=file_meta, key_column_names=key_column_names)
    if not table: return

    table = remove_column_spaces(table=table)

    table = add_id_key(table=table, key_column_names=key_column_names)

    dml_type = 'I' if file_meta['is_full_load'] else 'U'
    table = add_elt_columns(table=table, file_meta=file_meta, dml_type=dml_type)

    if is_pc and True:
        print('\n\nShowing Final Table: \n')
        table.show(5)

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


