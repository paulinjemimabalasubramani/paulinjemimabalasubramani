"""
Read all Pershing FWT files and migrate to the ADLS Gen 2

https://standardfiles.pershing.com/


Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys, re
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'client_account'
sys.domain_abbr = 'CA'

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import catch_error, data_settings, logger, mark_execution_end, config_path, is_pc
from modules.spark_functions import create_spark, read_csv, read_text, column_regex, remove_column_spaces, collect_column, \
    add_id_key, add_elt_columns
from modules.azure_functions import read_tableinfo_rows, tableinfo_name, get_firms_with_crd
from modules.snowflake_ddl import connect_to_snowflake, iterate_over_all_tables_snowflake, create_source_level_tables, snowflake_ddl_params
from modules.migrate_files import save_tableinfo_dict_and_cloud_file_history, process_all_files_with_incrementals, FirmCRDNumber, \
    get_key_column_names


from pprint import pprint
from collections import defaultdict
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark import StorageLevel
from pyspark.sql.functions import col, lit, to_date, to_timestamp



# %% Parameters

save_pershing_to_adls_flag = True
save_tableinfo_adls_flag = True

tableinfo_source = 'PERSHING'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))
schema_folder_path = os.path.join(config_path, 'pershing_schema')

table_special_records = defaultdict(dict)

pershing_strftime = r'%m/%d/%Y %H:%M:%S' # http://strftime.org/
date_start = datetime.strptime(data_settings.file_history_start_date[tableinfo_source], r'%Y-%m-%d')

header_str = 'BOF      PERSHING '
trailer_str = 'EOF      PERSHING '
schema_header_str = 'HEADER'
schema_trailer_str = 'TRAILER'
groupBy = ['account_number']

date_column_name = 'run_datetime'
key_column_names = get_key_column_names(date_column_name=date_column_name)

firms_source = 'FINRA'

logger.info({
    'tableinfo_source': tableinfo_source,
    'data_path_folder': data_path_folder,
    'schema_folder_path': schema_folder_path,
})



# %% Create Spark Session

spark = create_spark()
snowflake_ddl_params.spark = spark


# %% Get Firms that have CRD Number

firms = get_firms_with_crd(spark=spark, tableinfo_source=firms_source)

if is_pc: pprint(firms)



# %% get and pre-process schema

@catch_error(logger)
def get_pershing_schema(spark, schema_file_name:str):
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    schema_file_path = os.path.join(schema_folder_path, schema_file_name)
    if not os.path.isfile(schema_file_path):
        logger.warning(f'Schema file is not found: {schema_file_path}')
        return

    schema = read_csv(spark=spark, file_path=schema_file_path)
    schema = remove_column_spaces(schema)
    schema = schema.withColumn('field_name', F.lower(F.regexp_replace(F.trim(col('field_name')), column_regex, '_')))
    schema = schema.withColumn('record_name', F.upper(F.trim(col('record_name'))))
    schema = schema.withColumn('conditional_changes', F.upper(F.trim(col('conditional_changes'))))
    schema = schema.withColumn('position', F.trim(col('position')))

    schema = schema.where(
        (col('field_name').isNotNull()) & (~col('field_name').isin(['', 'not_used', '_', '__', 'n_a', 'na', 'none', 'null', 'value'])) &
        (col('position').contains('-')) &
        (col('record_name').isNotNull())
        )

    schema = schema.withColumn('position_start', F.split(col('position'), pattern='-').getItem(0).cast(IntegerType()))
    schema = schema.withColumn('position_end', F.split(col('position'), pattern='-').getItem(1).cast(IntegerType()))
    schema = schema.withColumn('length', col('position_end') - col('position_start') + lit(1))

    #schema.persist(StorageLevel.MEMORY_AND_DISK)
    return schema



# %% Get Header Schema

@catch_error(logger)
def get_header_schema():
    """
    Get Schema for Header section of Pershing files
    """
    selected_fields = ['position_start', 'position_end', 'length']

    schema = get_pershing_schema(spark=spark, schema_file_name='customer_acct_info.csv')
    if not schema: raise Exception('Main Schema file is not found!')

    header_schema = schema.where(col('record_name')==lit(schema_header_str)).select(['field_name', *selected_fields]).collect()
    header_schema = {r['field_name']: {field_name: r[field_name] for field_name in selected_fields} for r in header_schema}
    header_schema['form_name'] = header_schema.pop('customer_acct_info')
    return header_schema



header_schema = get_header_schema()



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
    if record_name == schema_header_str:
        for field_name, pos in header_schema.items():
            table = extract_field_from_fwt(table=table, field_name=field_name, position_start=pos['position_start'], length=pos['length'])
    else:
        schema = schema.where(col('record_name')==lit(record_name))
        if schema.rdd.isEmpty(): return

        for schema_row in schema.rdd.toLocalIterator():
            schema_dict = schema_row.asDict()
            table = extract_field_from_fwt(table=table, field_name=schema_dict['field_name'], position_start=schema_dict['position_start'], length=schema_dict['length'])

    table = table.drop('value')
    tables[(record_name, conditional_changes)] = table



# %% Find Field Position and Length

@catch_error(logger)
def find_field_position(schema, record_name:str, field_name:str):
    """
    Find field Position and Length from schema for a given record_name and field_name
    """
    field_schema = schema.where(
        (col('record_name')==lit(record_name)) &
        (col('field_name')==lit(field_name))
        ).select(['position_start', 'length']).collect()[0]

    position_start = field_schema['position_start']
    length = field_schema['length']

    if is_pc: pprint({
        'record_name': record_name,
        'field_name': field_name,
        'position_start': position_start,
        'length': length,
        })

    return position_start, length



# %% Get Distinct Field values

@catch_error(logger)
def get_dictinct_field_values(table, schema, record_name:str, field_name:str):
    """
    Get Distinct Field values from table for given record_name and field_name
    """
    position_start, length = find_field_position(schema=schema, record_name=record_name, field_name=field_name)
    table = extract_field_from_fwt(table=table, field_name=field_name, position_start=position_start, length=length)

    field_values = collect_column(table=table, column_name=field_name, distinct=True)
    return table, field_values



# %% Add Sub-tables to table

@catch_error(logger)
def add_sub_tables_to_table(tables, schema, sub_tables, record_name:str):
    """
    Add sub-tables from special_records (with conditional_changes) to the tables list
    """
    for conditional_changes, sub_table in sub_tables.items():
        #if sub_table.rdd.isEmpty(): continue

        filter_schema = schema.where(
            (col('record_name')==lit(record_name)) & (
                (col('conditional_changes').contains(conditional_changes.upper())) |
                (col('conditional_changes').isNull()) |
                (col('conditional_changes')==lit(''))
                )
            )

        add_schema_fields_to_table(tables=tables, table=sub_table, schema=filter_schema, record_name=record_name, conditional_changes=conditional_changes)



# %% generate tables from fixed width table file

@catch_error(logger)
def generate_tables_from_fwt(
        text_file,
        schema,
        table_name:str,
        header_str:str = header_str,
        trailer_str:str = trailer_str
        ):
    """
    Generate list of sub-tables from a fixed width table file based on record_name and conditional_changes
    """
    tables = dict()
    cv = col('value').substr

    special_records = table_special_records[table_name]
    record_names = collect_column(table=schema, column_name='record_name', distinct=True)

    for record_name in record_names:
        if is_pc: pprint(f'Record Name: {record_name}')

        if record_name == schema_header_str:
            table = text_file.where(cv(1, len(header_str))==lit(header_str))
        elif record_name == schema_trailer_str:
            table = text_file.where(cv(1, len(trailer_str))==lit(trailer_str))
        else:
            table = text_file.where((cv(3, 1)==lit(record_name)) & (cv(1, len(header_str))!=lit(header_str)) & (cv(1, len(trailer_str))!=lit(trailer_str)))

        #if table.rdd.isEmpty(): continue

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
def combine_rows_into_array(tables, groupBy:list):
    """
    Utility function to combine rows into Array (so that to have one column of json data per record name in the final main table)
    """
    for record_name in tables.keys():
        if record_name not in [schema_header_str, schema_trailer_str]:
            tables[record_name] = (tables[record_name]
                .withColumn('all_columns', F.struct(tables[record_name].columns))
                .groupBy(groupBy)
                .agg(F.collect_list('all_columns').alias(f'record_{record_name}'))
                )

    return tables



# %% Join All Tables

@catch_error(logger)
def join_all_tables(tables, groupBy:list):
    """
    Join all sub-tables that were split by record_name and conditional_changes
    """
    joined_tables = None

    for record_name, table in tables.items():
        if record_name not in [schema_header_str, schema_trailer_str]:
            if joined_tables:
                joined_tables = joined_tables.join(table, on=groupBy, how='full')
            else:
                joined_tables = table

    header = tables[schema_header_str].collect()[0]
    header_map = ['date_of_data', 'remote_id', 'run_date', 'run_time', 'refreshed_updated']
    for column_name in header_map:
        joined_tables = joined_tables.withColumn(column_name, lit(header[column_name]))

    #joined_tables.persist(StorageLevel.MEMORY_AND_DISK)
    return joined_tables



# %% Create Table from Fixed-With Table File

@catch_error(logger)
def create_table_from_fwt_file(firm_path_folder:str, file_name:str, schema_file_name:str, table_name:str, groupBy:list):
    """
    Create Table from Fixed-With Table File. Convery FWT to nested Spark table.
    """
    data_file_path = os.path.join(firm_path_folder, file_name)

    logger.info({
        'action': 'generate_tables_from_fwt_file',
        'data_file_path': data_file_path,
        'schema_file_name': schema_file_name,
    })

    schema = get_pershing_schema(spark=spark, schema_file_name=schema_file_name)
    if not schema: return

    text_file = read_text(spark=spark, file_path=data_file_path)

    tables = generate_tables_from_fwt(text_file=text_file, schema=schema, table_name=table_name)
    tables = union_tables_per_record(tables=tables)
    tables = combine_rows_into_array(tables=tables, groupBy=groupBy)

    table = join_all_tables(tables=tables, groupBy=groupBy)
    return table



# %% Process customer_acct_info Record A

@catch_error(logger)
def process_record_A_customer_acct_info(table, schema, record_name):
    """
    Special Record processing for customer_acct_info Record A - because of it has conditional_changes
    """
    if record_name != 'A': return

    registration_type_field_name = 'registration_type'
    table, registration_types = get_dictinct_field_values(table=table, schema=schema, record_name=record_name, field_name=registration_type_field_name)

    sub_tables = {registration_type:table.where(col(registration_type_field_name)==lit(registration_type)) for registration_type in registration_types}
    return sub_tables


table_special_records['customer_acct_info']['A'] = process_record_A_customer_acct_info



# %% Process customer_acct_info Record C

@catch_error(logger)
def process_record_C_customer_acct_info(table, schema, record_name):
    """
    Special Record processing for customer_acct_info Record C - because of it has conditional_changes
    """
    if record_name != 'C': return

    country_field_name = 'country_code_1'
    table, countries = get_dictinct_field_values(table=table, schema=schema, record_name=record_name, field_name=country_field_name)

    USCA_codes = ['US','CA']

    sub_tables = {
        'US or Canada addresses only': table.where(col(country_field_name).isin(USCA_codes)),
        'Non-US or Canada addresses only': table.where(~col(country_field_name).isin(USCA_codes)),
    }
    return sub_tables


table_special_records['customer_acct_info']['C'] = process_record_C_customer_acct_info



# %% Extract Meta Data from Pershing FWT file

@catch_error(logger)
def extract_pershing_file_meta(file_path:str, firm_crd_number:str, cloud_file_history):
    """
    Extract Meta Data from Pershing FWT file (reading 1st line (header metadata) from inside the file)
    """
    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    if HEADER[:len(header_str)] != header_str: 
        logger.warning(f'Not a Pershing file: {file_path}')
        return

    file_meta = {
        'file_name': os.path.basename(file_path),
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        FirmCRDNumber: firm_crd_number,
    }

    for field_name, pos in header_schema.items():
        file_meta[field_name] = re.sub(' +', ' ', HEADER[pos['position_start']-1: pos['position_end']].strip())

    file_meta['table_name'] = re.sub(' ', '_', file_meta['form_name'].lower())
    file_meta['schema_file_name'] = file_meta['table_name'] + '.csv'
    file_meta['is_full_load'] = file_meta['refreshed_updated'].upper() == 'REFRESHED'
    file_meta[date_column_name] = datetime.strptime(' '.join([file_meta['run_date'], file_meta['run_time']]), pershing_strftime)
    return file_meta



# %% Main Processing of Pershing File

@catch_error(logger)
def process_pershing_file(file_meta, cloud_file_history):
    """
    Main Processing of single Pershing file
    """
    logger.info(file_meta)
    firm_path_folder = os.path.join(data_path_folder, file_meta[FirmCRDNumber])

    table = create_table_from_fwt_file(
        firm_path_folder = firm_path_folder,
        file_name = file_meta['file_name'],
        schema_file_name = file_meta['schema_file_name'],
        table_name = file_meta['table_name'],
        groupBy = groupBy,
        )
    if not table: return

    table = remove_column_spaces(table=table)
    table = table.withColumn(date_column_name, lit(str(file_meta[date_column_name])))
    table = table.withColumn(FirmCRDNumber, lit(str(file_meta[FirmCRDNumber])))

    table = add_id_key(table, key_column_names=groupBy)

    table = add_elt_columns(
        table = table,
        reception_date = file_meta[date_column_name],
        source = tableinfo_source,
        is_full_load = file_meta['is_full_load'],
        dml_type = 'I' if file_meta['is_full_load'] else 'U',
        )

    if is_pc: table.show(5)
    if is_pc: print(f'Number of rows: {table.count()}')

    return {file_meta['table_name']: table}



# %% Iterate over all the files in all the firms and process them.

additional_ingest_columns = [
    to_timestamp(col(date_column_name)).alias(date_column_name, metadata={'sqltype': '[datetime] NULL'}),
    to_date(col('date_of_data'), format='MM/dd/yyyy').alias('date_of_data', metadata={'sqltype': '[date] NULL'}),
    col('remote_id').cast(StringType()).alias('remote_id', metadata={'maxlength': 50, 'sqltype': 'varchar(50)'}),
    col('form_name').cast(StringType()).alias('form_name', metadata={'maxlength': 50, 'sqltype': 'varchar(50)'}),
    col('schema_file_name').cast(StringType()).alias('schema_file_name', metadata={'maxlength': 300, 'sqltype': 'varchar(300)'}),
    ]

all_new_files, PARTITION_list, tableinfo = process_all_files_with_incrementals(
    spark = spark,
    firms = firms,
    data_path_folder = data_path_folder,
    fn_extract_file_meta = extract_pershing_file_meta,
    date_start = date_start,
    additional_ingest_columns = additional_ingest_columns,
    fn_process_file = process_pershing_file,
    key_column_names = key_column_names,
    tableinfo_source = tableinfo_source,
    save_data_to_adls_flag = save_pershing_to_adls_flag,
    date_column_name = date_column_name
)



# %% Save Tableinfo metadata table into Azure and Save Ingest files metadata to SQL Server.

tableinfo = save_tableinfo_dict_and_cloud_file_history(
    spark = spark,
    tableinfo = tableinfo,
    tableinfo_source = tableinfo_source,
    all_new_files = all_new_files,
    save_tableinfo_adls_flag = save_tableinfo_adls_flag,
    )



# %% Read metadata.TableInfo

table_rows = read_tableinfo_rows(tableinfo_name=tableinfo_name, tableinfo_source=tableinfo_source, tableinfo=tableinfo)


# %% Connect to SnowFlake

snowflake_connection = connect_to_snowflake()
snowflake_ddl_params.snowflake_connection = snowflake_connection


# %% Iterate Over Steps for all tables

ingest_data_list = iterate_over_all_tables_snowflake(tableinfo=tableinfo, table_rows=table_rows, PARTITION_list=PARTITION_list)


# %% Create Source Level Tables

create_source_level_tables(ingest_data_list=ingest_data_list)


# %% Close Showflake connection

snowflake_connection.close()


# %% Mark Execution End

mark_execution_end()


# %%

