"""
Read Pershing "CUSTOMER ACCOUNT INFORMATION" - ACCT (Update File) / ACCF (Refresh File)

https://standardfiles.pershing.com/


Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

__file__ = r'C:\Users\smammadov\packages\EDIP-Code\src\pershing_accf.py'


import os, sys
from re import L
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'client_and_account'
sys.domain_abbr = 'CA'

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import catch_error, data_settings, get_secrets, logger, mark_execution_end, config_path, is_pc
from modules.spark_functions import create_spark, read_csv, read_text, column_regex, remove_column_spaces
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo_rows, default_storage_account_name, tableinfo_name

from pprint import pprint

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel



# %% Parameters

storage_account_name = default_storage_account_name
tableinfo_source = 'ACCF'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))
schema_folder_path = os.path.join(config_path, 'pershing_schema')

logger.info({
    'tableinfo_source': tableinfo_source,
    'data_path_folder': data_path_folder,
    'schema_folder_path': schema_folder_path,
})



# %% Create Session

spark = create_spark()



# %% pre-process schema

@catch_error(logger)
def preprocess_schema(schema):
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



# %% Add Field to a fixed width table

@catch_error(logger)
def add_field_to_fwt(table, field_name:str, position_start:int, length:int):
    cv = col('value').substr
    return table.withColumn(field_name, F.regexp_replace(F.trim(cv(position_start, length)), ' +', ' '))



# %% Add Schema fields to table

@catch_error(logger)
def add_schema_fields_to_table(tables, table, schema, record_name:str, conditional_changes:str=''):
    schema = schema.where(col('record_name')==lit(record_name))
    if schema.count()==0: return

    for schema_row in schema.rdd.toLocalIterator():
        schema_dict = schema_row.asDict()
        table = add_field_to_fwt(table=table, field_name=schema_dict['field_name'], position_start=schema_dict['position_start'], length=schema_dict['length'])

    tables[(record_name, conditional_changes)] = table


# %% Find Field Position and Length

@catch_error(logger)
def find_field_position(schema, record_name:str, field_name:str):
    field_schema = schema.where(
        (col('record_name')==lit(record_name)) &
        (col('field_name')==lit(field_name))
        ).select(['position_start', 'length']).collect()

    position_start = field_schema[0]['position_start']
    length = field_schema[0]['length']

    if is_pc: pprint({
        'record_name': record_name,
        'field_name': field_name,
        'position_start': position_start,
        'length': length,
        })

    return position_start, length



# %% Get Distict Field values

@catch_error(logger)
def get_dictinct_field_values(table, schema, record_name:str, field_name:str):
    position_start, length = find_field_position(schema=schema, record_name=record_name, field_name=field_name)
    table = add_field_to_fwt(table=table, field_name=field_name, position_start=position_start, length=length)

    field_values = table.select(field_name).distinct().collect()
    field_values = [x[field_name] for x in field_values]

    return table, field_values


# %% Add Sub-tables to table

@catch_error(logger)
def add_sub_tables_to_table(tables, schema, sub_tables, record_name:str):
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




# %%

@catch_error(logger)
def generate_tables_from_fwt(
        text_file,
        schema,
        special_records:dict,
        header_str:str = 'BOF      PERSHING ',
        trailer_str:str = 'EOF      PERSHING ',
        transaction_code:str = 'CI',
        ):

    tables = dict()
    cv = col('value').substr

    record_names = schema.select('record_name').distinct().collect()
    record_names = [x['record_name'] for x in record_names]

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
            special_records[record_name](tables=tables, table=table, schema=schema, record_name=record_name)
        else:
            add_schema_fields_to_table(tables=tables, table=table, schema=schema, record_name=record_name)

    return tables



# %% Generate Tables from Fixed-With Table File

@catch_error(logger)
def generate_tables_from_fwt_file(file_name:str, schema_file_name:str, special_records):
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
    return tables



# %% Process ACCF Record A

@catch_error(logger)
def process_record_A_accf(tables, table, schema, record_name):
    if record_name != 'A': return

    registration_type_field_name = 'registration_type'
    table, registration_types = get_dictinct_field_values(table=table, schema=schema, record_name=record_name, field_name=registration_type_field_name)

    sub_tables = {registration_type:table.where(col(registration_type_field_name)==lit(registration_type)) for registration_type in registration_types}

    add_sub_tables_to_table(tables=tables, schema=schema, sub_tables=sub_tables, record_name=record_name)



# %% Process ACCF Record C

@catch_error(logger)
def process_record_C_accf(tables, table, schema, record_name):
    if record_name != 'C': return

    country_field_name = 'country_code_1'
    table, countries = get_dictinct_field_values(table=table, schema=schema, record_name=record_name, field_name=country_field_name)

    USCA_codes = ['US','CA']

    sub_tables = {
        'US or Canada addresses only': table.where(col(country_field_name).isin(USCA_codes)),
        'Non-US or Canada addresses only': table.where(~col(country_field_name).isin(USCA_codes)),
    }

    add_sub_tables_to_table(tables=tables, schema=schema, sub_tables=sub_tables, record_name=record_name)



# %% Process ACCF File

special_records = {
    'A': process_record_A_accf,
    'C': process_record_C_accf,
    }


tables = generate_tables_from_fwt_file(
    file_name = '4CCF.4CCF',
    schema_file_name = 'customer_account_information_acct_accf.csv',
    special_records = special_records,
)








# %% Mark Execution End

mark_execution_end()


# %%






