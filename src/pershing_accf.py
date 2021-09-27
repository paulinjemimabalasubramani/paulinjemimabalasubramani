"""
Read Pershing "CUSTOMER ACCOUNT INFORMATION" - ACCT (Update File) / ACCF (Refresh File)

https://standardfiles.pershing.com/


Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'client_and_account'
sys.domain_abbr = 'CA'

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import catch_error, data_settings, get_secrets, logger, mark_execution_end, config_path, is_pc
from modules.spark_functions import create_spark, read_csv, read_text, column_regex, remove_column_spaces, collect_column
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo_rows, default_storage_account_name, tableinfo_name, \
    add_table_to_tableinfo, default_storage_account_abbr, azure_container_folder_path, data_folder

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
schema_name = tableinfo_source.lower()

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))
schema_folder_path = os.path.join(config_path, 'pershing_schema')

logger.info({
    'tableinfo_source': tableinfo_source,
    'data_path_folder': data_path_folder,
    'schema_folder_path': schema_folder_path,
})

tableinfo = defaultdict(list)



# %% Create Session

spark = create_spark()



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
    if schema.count()==0: return

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
        if sub_table.count()==0: continue

        filter_schema = schema.where(
            (col('record_name')==lit(record_name)) & (
                (col('conditional_changes').contains(conditional_changes.upper())) |
                (col('conditional_changes').isNull()) |
                (col('conditional_changes')==lit(''))
                )
            )

        add_schema_fields_to_table(tables=tables, table=sub_table, schema=filter_schema, record_name=record_name, conditional_changes=conditional_changes)



# %% generate tables from fixed with table file

@catch_error(logger)
def generate_tables_from_fwt(
        text_file,
        schema,
        special_records:dict,
        header_str:str = 'BOF      PERSHING ',
        trailer_str:str = 'EOF      PERSHING ',
        transaction_code:str = 'CI',
        ):
    """
    Generate list of sub-tables from a fixed with table file based on record_name and conditional_changes
    """

    tables = dict()
    cv = col('value').substr

    record_names = collect_column(table=schema, column_name='record_name', distinct=True)

    for record_name in record_names:
        if is_pc: pprint(f'Record Name: {record_name}')

        if record_name == 'HEADER':
            table = text_file.where(cv(1, len(header_str))==lit(header_str))
        elif record_name == 'TRAILER':
            table = text_file.where(cv(1, len(trailer_str))==lit(trailer_str))
        else:
            table = text_file.where(
                (cv(1, len(transaction_code))==lit(transaction_code)) &
                (cv(3, 1)==lit(record_name))
                )

        if table.count()==0: continue

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
        if record_name not in ['HEADER', 'TRAILER']:
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
        if record_name not in ['HEADER', 'TRAILER']:
            if joined_tables:
                joined_tables = joined_tables.join(table, on=groupBy, how='full')
            else:
                joined_tables = table

    header = tables['HEADER'].collect()[0]
    header_map = ['date_of_data', 'remote_id', 'run_date', 'run_time', 'refreshed_updated']
    for column_name in header_map:
        joined_tables = joined_tables.withColumn(column_name, lit(header[column_name]))

    return joined_tables



# %% Create Table from Fixed-With Table File

@catch_error(logger)
def create_table_from_fwt_file(file_name:str, schema_file_name:str, special_records, groupBy):
    """
    Create Table from Fixed-With Table File. Convery FWT to nested Spark table.
    """
    schema_file_path = os.path.join(schema_folder_path, schema_file_name)
    data_file_path = os.path.join(data_path_folder, file_name)

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



# %% Process ACCF Record A

@catch_error(logger)
def process_record_A_accf(table, schema, record_name):
    """
    Special Record processing for ACCF Record A - because of it has conditional_changes
    """
    if record_name != 'A': return

    registration_type_field_name = 'registration_type'
    table, registration_types = get_dictinct_field_values(table=table, schema=schema, record_name=record_name, field_name=registration_type_field_name)

    sub_tables = {registration_type:table.where(col(registration_type_field_name)==lit(registration_type)) for registration_type in registration_types}
    return sub_tables



# %% Process ACCF Record C

@catch_error(logger)
def process_record_C_accf(table, schema, record_name):
    """
    Special Record processing for ACCF Record C - because of it has conditional_changes
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



# %% Process ACCF File

table_name = 'accf'

special_records = {
    'A': process_record_A_accf,
    'C': process_record_C_accf,
    }


table = create_table_from_fwt_file(
    file_name = '4CCF.4CCF',
    schema_file_name = 'customer_account_information_acct_accf.csv',
    special_records = special_records,
    groupBy = ['account_number'],
)


if is_pc: table.show(5)



# %% Write Table to Azure

add_table_to_tableinfo(
    tableinfo = tableinfo, 
    table = table, 
    schema_name = schema_name, 
    table_name = table_name, 
    tableinfo_source = tableinfo_source, 
    storage_account_name = storage_account_name,
    storage_account_abbr = storage_account_abbr,
    )


container_folder = azure_container_folder_path(data_type=data_folder, domain_name=sys.domain_name, source_or_database=tableinfo_source, firm_or_schema=schema_name)


if is_pc: pprint(container_folder)


# TODO: Write to Azure



# %% Mark Execution End

mark_execution_end()


# %%


