"""
Read all Pershing FWT files and migrate to the ADLS Gen 2

https://standardfiles.pershing.com/


Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'client_account'
sys.domain_abbr = 'CA'

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import catch_error, data_settings, get_secrets, logger, mark_execution_end, config_path, is_pc
from modules.spark_functions import create_spark, read_csv, read_text, column_regex, remove_column_spaces, collect_column, \
    get_sql_table_names, read_sql
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo_rows, default_storage_account_name, tableinfo_name, \
    add_table_to_tableinfo, default_storage_account_abbr, azure_container_folder_path, data_folder, get_firms_with_crd

from pprint import pprint
from collections import defaultdict

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel



# %% Parameters

storage_account_name = default_storage_account_name
storage_account_abbr = default_storage_account_abbr
tableinfo_source = 'PERSHING'

sql_server = 'DSQLOLTP02'
sql_database = 'EDIPIngestion'
sql_schema = 'edip'
sql_ingest_table_name = f'{tableinfo_source.lower()}_ingest'
sql_key_vault_account = 'sqledipingestion'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))
schema_folder_path = os.path.join(config_path, 'pershing_schema')

logger.info({
    'tableinfo_source': tableinfo_source,
    'data_path_folder': data_path_folder,
    'schema_folder_path': schema_folder_path,
})

tableinfo = defaultdict(list)

header_str = 'BOF      PERSHING '
trailer_str = 'EOF      PERSHING '
schema_header_str = 'HEADER'
schema_trailer_str = 'TRAILER'



# %% Create Session

spark = create_spark()


# %% Read Key Vault Data

_, sql_id, sql_pass = get_secrets(sql_key_vault_account.lower(), logger=logger)



# %% Get Firms that have CRD Number

firms = get_firms_with_crd(spark=spark, tableinfo_source='FINRA')

if is_pc: pprint(firms)



# %% Get SQL Ingest Table

table_names, sql_tables = get_sql_table_names(
    spark = spark, 
    schema = sql_schema,
    database = sql_database,
    server = sql_server,
    user = sql_id,
    password = sql_pass,
    )

sql_ingest_table_exists = sql_ingest_table_name in table_names

sql_ingest_table = None
if sql_ingest_table_exists:
    sql_ingest_table = read_sql(
        spark = spark, 
        user = sql_id, 
        password = sql_pass, 
        schema = sql_schema, 
        table_name = sql_ingest_table_name, 
        database = sql_database, 
        server = sql_server
        )



# %% pre-process schema

@catch_error(logger)
def preprocess_schema(schema):
    """
    Pre-process the schema table to make it code-friendly
    """
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

    schema.persist(StorageLevel.MEMORY_AND_DISK)
    return schema



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
    schema = schema.where(col('record_name')==lit(record_name))
    if schema.rdd.isEmpty(): return

    for schema_row in schema.rdd.toLocalIterator():
        schema_dict = schema_row.asDict()
        table = extract_field_from_fwt(table=table, field_name=schema_dict['field_name'], position_start=schema_dict['position_start'], length=schema_dict['length'])

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
        special_records:dict,
        header_str:str = header_str,
        trailer_str:str = trailer_str,
        transaction_code:str = 'CI',
        ):
    """
    Generate list of sub-tables from a fixed width table file based on record_name and conditional_changes
    """

    tables = dict()
    cv = col('value').substr

    record_names = collect_column(table=schema, column_name='record_name', distinct=True)

    for record_name in record_names:
        if is_pc: pprint(f'Record Name: {record_name}')

        if record_name == schema_header_str:
            table = text_file.where(cv(1, len(header_str))==lit(header_str))
        elif record_name == schema_trailer_str:
            table = text_file.where(cv(1, len(trailer_str))==lit(trailer_str))
        else:
            table = text_file.where(
                (cv(1, len(transaction_code))==lit(transaction_code)) &
                (cv(3, 1)==lit(record_name))
                )

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
def combine_rows_into_array(tables, groupBy):
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
def join_all_tables(tables, groupBy):
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

    joined_tables.persist(StorageLevel.MEMORY_AND_DISK)
    return joined_tables



# %% Create Table from Fixed-With Table File

@catch_error(logger)
def create_table_from_fwt_file(firm_path_folder:str, file_name:str, schema_file_name:str, special_records, groupBy):
    """
    Create Table from Fixed-With Table File. Convery FWT to nested Spark table.
    """
    schema_file_path = os.path.join(schema_folder_path, schema_file_name)
    data_file_path = os.path.join(firm_path_folder, file_name)

    logger.info({
        'action': 'generate_tables_from_fwt_file',
        'data_file_path': data_file_path,
        'schema_file_path': schema_file_path,
    })

    schema = read_csv(spark=spark, file_path=schema_file_path)
    schema = preprocess_schema(schema=schema)

    text_file = read_text(spark=spark, file_path=data_file_path)

    tables = generate_tables_from_fwt(text_file=text_file, schema=schema, special_records=special_records)
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



# %% Get Header Schema

@catch_error(logger)
def get_header_schema():
    """
    Get Schema for Header section of Pershing files
    """
    schema_file_path = os.path.join(schema_folder_path, 'customer_acct_info.csv')
    schema = read_csv(spark=spark, file_path=schema_file_path)
    schema = preprocess_schema(schema=schema)
    header_schema = schema.where(col('record_name')==lit(schema_header_str)).select(['field_name', 'position_start', 'position_end']).collect()

    header_dict = dict()
    for r in header_schema:
        header_dict[r['field_name']] = {
            'position_start': r['position_start'],
            'position_start': r['position_end'],
        }

    header_dict['form_name'] = header_dict.pop('customer_acct_info')

    return header_dict



header_schema = get_header_schema()



# %% Get Meta Data from Pershing FWT file

@catch_error(logger)
def get_pershing_file_meta(file_path:str, firm_crd_number:str):
    """
    Get Meta Data from Pershing FWT file (reading metadata from inside the file - 1st line Header)
    """
    global sql_ingest_table_exists, sql_ingest_table

    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    if HEADER[:len(header_str)] != header_str: return

    file_meta = dict()

    for field_name, pos in header_schema.items():
        file_meta[field_name] = HEADER[pos['position_start']-1: pos['position_end']]

    file_meta['firm_crd_number'] = firm_crd_number
    return file_meta



firm_crd_number = '23131'
file_name = '4CCF.4CCF'

firm_path_folder = os.path.join(data_path_folder, firm_crd_number)
file_path = os.path.join(firm_path_folder, file_name)


file_meta = get_pershing_file_meta(file_path=file_path, firm_crd_number=firm_crd_number)

pprint(file_meta)



# %%










# %% Process customer_acct_info

firm_name = 'RAA'
firm_crd_number = '23131'

file_name = '4CCF.4CCF'
schema_file_name = 'customer_acct_info.csv'

table_name = 'accf'
groupBy = ['account_number']

special_records = {
    'A': process_record_A_customer_acct_info,
    'C': process_record_C_customer_acct_info,
    }


table = create_table_from_fwt_file(
    firm_path_folder = os.path.join(data_path_folder, firm_crd_number),
    file_name = file_name,
    schema_file_name = schema_file_name,
    special_records = special_records,
    groupBy = groupBy,
)


if is_pc: table.show(5)
if is_pc: print(f'Number of rows: {table.count()}')


# %% Write Table to Azure

add_table_to_tableinfo(
    tableinfo = tableinfo, 
    table = table, 
    schema_name = firm_name, 
    table_name = table_name, 
    tableinfo_source = tableinfo_source, 
    storage_account_name = storage_account_name,
    storage_account_abbr = storage_account_abbr,
    )


container_folder = azure_container_folder_path(data_type=data_folder, domain_name=sys.domain_name, source_or_database=tableinfo_source, firm_or_schema=firm_name)


if is_pc: pprint(container_folder)


# TODO: Write to Azure



# %% Mark Execution End

mark_execution_end()


# %%


