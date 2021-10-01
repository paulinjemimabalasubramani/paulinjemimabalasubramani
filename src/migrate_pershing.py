"""
Read all Pershing FWT files and migrate to the ADLS Gen 2

https://standardfiles.pershing.com/


Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys, re, tempfile, shutil, copy
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'client_account'
sys.domain_abbr = 'CA'

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import catch_error, data_settings, get_secrets, logger, mark_execution_end, config_path, is_pc, \
    execution_date
from modules.spark_functions import create_spark, read_csv, read_text, column_regex, remove_column_spaces, collect_column, \
    get_sql_table_names, read_sql, add_md5_key, write_sql, MD5KeyIndicator, partitionBy, IDKeyIndicator, add_id_key, \
    add_elt_columns
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo_rows, default_storage_account_name, tableinfo_name, \
    add_table_to_tableinfo, default_storage_account_abbr, azure_container_folder_path, data_folder, get_firms_with_crd, \
    to_storage_account_name, save_adls_gen2, container_name, file_format, select_tableinfo_columns, tableinfo_container_name, \
    metadata_folder
from modules.snowflake_ddl import connect_to_snowflake, iterate_over_all_tables_snowflake, create_source_level_tables, snowflake_ddl_params


from pprint import pprint
from collections import defaultdict
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark import StorageLevel
from pyspark.sql.functions import col, lit, to_date, to_timestamp, row_number, when
from pyspark.sql.window import Window



# %% Parameters

save_pershing_to_adls_flag = True
save_tableinfo_adls_flag = True

if not is_pc:
    save_pershing_to_adls_flag = True
    save_tableinfo_adls_flag = True

storage_account_name = default_storage_account_name
storage_account_abbr = default_storage_account_abbr
tableinfo_source = 'PERSHING'
database = tableinfo_source

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
table_special_records = defaultdict(dict)
PARTITION_list = defaultdict(str)
tmpdirs = []

pershing_strftime = r'%m/%d/%Y %H:%M:%S' # http://strftime.org/
date_start = datetime.strptime('01/01/1990 00:00:00', pershing_strftime)

header_str = 'BOF      PERSHING '
trailer_str = 'EOF      PERSHING '
schema_header_str = 'HEADER'
schema_trailer_str = 'TRAILER'
groupBy = ['account_number']

key_column_names = ['firm_crd_number', 'table_name']
key_column_names_with_load = key_column_names + ['is_full_load']
key_column_names_with_load_n_date = key_column_names_with_load + ['run_datetime']



# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark



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

    schema.persist(StorageLevel.MEMORY_AND_DISK)
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
        trailer_str:str = trailer_str,
        transaction_code:str = 'CI',
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

    joined_tables.persist(StorageLevel.MEMORY_AND_DISK)
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
def extract_pershing_file_meta(file_path:str, firm_crd_number:str):
    """
    Extract Meta Data from Pershing FWT file (reading 1st line (header metadata) from inside the file)
    """
    global sql_ingest_table_exists, sql_ingest_table

    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    if HEADER[:len(header_str)] != header_str: 
        logger.warning(f'Not a Pershing file: {file_path}')
        return

    file_meta = {
        'file_name': os.path.basename(file_path),
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'firm_crd_number': firm_crd_number,
    }

    for field_name, pos in header_schema.items():
        file_meta[field_name] = re.sub(' +', ' ', HEADER[pos['position_start']-1: pos['position_end']].strip())

    file_meta['table_name'] = re.sub(' ', '_', file_meta['form_name'].lower())
    file_meta['schema_file_name'] = file_meta['table_name'] + '.csv'
    file_meta['is_full_load'] = file_meta['refreshed_updated'].upper() == 'REFRESHED'
    file_meta['run_datetime'] = datetime.strptime(' '.join([file_meta['run_date'], file_meta['run_time']]), pershing_strftime)
    return file_meta



# %% Get Meta Data from Pershing FWT file

@catch_error(logger)
def get_pershing_file_meta(file_path:str, firm_crd_number:str):
    """
    Get Meta Data from Pershing FWT file (reading 1st line (header metadata) from inside the file)
    Handles Zip files extraction as well
    """
    if file_path.lower().endswith('.zip'):
        with tempfile.TemporaryDirectory(dir=os.path.dirname(file_path)) as tmpdir:
            shutil.unpack_archive(filename=file_path, extract_dir=tmpdir)
            k = 0
            for root1, dirs1, files1 in os.walk(tmpdir):
                for file1 in files1:
                    file_path1 = os.path.join(root1, file1)
                    file_meta = extract_pershing_file_meta(file_path=file_path1, firm_crd_number=firm_crd_number)
                    k += 1
                    break
                if k>0: break

    else:
        file_meta = extract_pershing_file_meta(file_path=file_path, firm_crd_number=firm_crd_number)

    return file_meta



# %% Get all files meta data for a given folder_path

@catch_error(logger)
def get_all_pershing_file_meta(folder_path, date_start:datetime, firm_crd_number:str, inclusive:bool=True):
    """
    Get all files meta data for a given folder_path.
    Filters files for dates after the given date_start
    """
    logger.info(f'Getting list of candidate files from {folder_path}')
    files_meta = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_meta = get_pershing_file_meta(file_path=file_path, firm_crd_number=firm_crd_number)

            if file_meta and (date_start<file_meta['run_datetime'] or (date_start==file_meta['run_datetime'] and inclusive)):
                files_meta.append(file_meta)

    logger.info(f'Finished getting list of files. Total Files = {len(files_meta)}')
    return files_meta



# %% Remove temporary folders

@catch_error(logger)
def cleanup_tmpdirs():
    """
    Remove temporary folders created by unzip process
    """
    global tmpdirs
    while len(tmpdirs)>0:
        tmpdirs.pop(0).cleanup()



# %% Create Ingest Table from files_meta

@catch_error(logger)
def ingest_table_from_files_meta(files_meta, firm_name:str, storage_account_name:str, storage_account_abbr:str):
    """
    Create Ingest Table from files metadata. This will ensure not to ingest the same file 2nd time in future.
    """
    global sql_ingest_table_exists, sql_ingest_table

    ingest_table = spark.createDataFrame(files_meta)
    ingest_table = ingest_table.select(
        col('firm_crd_number').cast(StringType()),
        col('table_name').cast(StringType()),
        to_date(col('date_of_data'), format='MM/dd/yyyy').alias('date_of_data'),
        to_timestamp(col('run_datetime')).alias('run_datetime'),
        col('is_full_load').cast(BooleanType()),
        lit(firm_name).cast(StringType()).alias('firm_name'),
        lit(storage_account_name).cast(StringType()).alias('storage_account_name'),
        lit(storage_account_abbr).cast(StringType()).alias('storage_account_abbr'),
        lit(None).cast(StringType()).alias('remote_source'),
        col('folder_path').cast(StringType()).alias('folder_path'),
        col('file_name').cast(StringType()).alias('file_name'),
        to_timestamp(lit(None)).alias('ingestion_date'), # execution_date
        lit(True).cast(BooleanType()).alias('is_ingested'),
        to_timestamp(lit(None)).alias('full_load_date'),
        col('remote_id').cast(StringType()).alias('remote_id'),
        col('form_name').cast(StringType()).alias('form_name'),
    )

    ingest_table = add_md5_key(ingest_table, key_column_names=key_column_names_with_load_n_date)

    if not sql_ingest_table_exists:
        sql_ingest_table_exists = True
        sql_ingest_table = spark.createDataFrame(spark.sparkContext.emptyRDD(), ingest_table.schema)
        write_sql(
            table = sql_ingest_table,
            table_name = sql_ingest_table_name,
            schema = sql_schema,
            database = sql_database,
            server = sql_server,
            user = sql_id,
            password = sql_pass,
            mode = 'overwrite',
        )

    return ingest_table



# %% Get Selected Files

@catch_error(logger)
def get_selected_files(ingest_table):
    """
    Select only the files that need to be ingested this time.
    """
    # Union all files
    new_files = ingest_table.alias('t'
        ).join(sql_ingest_table, ingest_table[MD5KeyIndicator]==sql_ingest_table[MD5KeyIndicator], how='left_anti'
        ).select('t.*')

    union_columns = new_files.columns
    all_files = new_files.select(union_columns).union(sql_ingest_table.select(union_columns)).sort(key_column_names_with_load_n_date)

    # Filter out old files
    full_files = all_files.where('is_full_load').withColumn('row_number', 
            row_number().over(Window.partitionBy(key_column_names_with_load).orderBy(col('run_datetime').desc(), col(MD5KeyIndicator).desc()))
        ).where(col('row_number')==lit(1))

    all_files = all_files.alias('a').join(full_files.alias('f'), key_column_names, how='left').select('a.*', col('f.run_datetime').alias('max_date')) \
        .withColumn('full_load_date', when(col('full_load_date').isNull(), col('max_date')).otherwise(col('full_load_date'))).drop('max_date')

    all_files = all_files.withColumn('is_ingested', 
            when(col('ingestion_date').isNull() & col('full_load_date').isNotNull() & (col('run_datetime')<col('full_load_date')), lit(False)).otherwise(col('is_ingested'))
        )

    # Select and Order Files for ingestion
    new_files = all_files.where(col('ingestion_date').isNull()).withColumn('ingestion_date', to_timestamp(lit(execution_date)))
    selected_files = new_files.where('is_ingested').orderBy(col('firm_crd_number').desc(), col('table_name').desc(), col('is_full_load').desc(), col('run_datetime').asc())

    return new_files, selected_files



# %% Main Processing of Pershing File

@catch_error(logger)
def process_pershing_file(file_meta):
    """
    Main Processing of single Pershing file
    """
    file_path = os.path.join(file_meta['folder_path'], file_meta['file_name'])

    logger.info(file_meta)

    firm_crd_number = file_meta['firm_crd_number']
    file_name = file_meta['file_name']

    firm_path_folder = os.path.join(data_path_folder, firm_crd_number)
    file_path = os.path.join(firm_path_folder, file_name)

    file_meta = extract_pershing_file_meta(file_path=file_path, firm_crd_number=firm_crd_number)
    table_name = file_meta['table_name']

    table = create_table_from_fwt_file(
        firm_path_folder = os.path.join(data_path_folder, firm_crd_number),
        file_name = file_name,
        schema_file_name = file_meta['schema_file_name'],
        table_name = table_name,
        groupBy=groupBy,
    )
    if not table: return

    table = add_id_key(table, key_column_names=groupBy)

    table = add_elt_columns(
        table = table,
        reception_date = file_meta['run_datetime'],
        source = tableinfo_source,
        is_full_load = file_meta['is_full_load'],
        dml_type = 'I' if file_meta['is_full_load'] else 'U',
        )

    if is_pc: table.show(5)
    if is_pc: print(f'Number of rows: {table.count()}')

    return {file_meta['table_name']: table}



# %% Process Single File

@catch_error(logger)
def process_one_file(file_meta):
    """
    Process Single File - Unzipped or ZIP file
    """
    global tmpdirs
    file_path = os.path.join(file_meta['folder_path'], file_meta['file_name'])
    logger.info(f'Processing {file_path}')

    if file_path.lower().endswith('.zip'):
        tmpdir = tempfile.TemporaryDirectory(dir=os.path.dirname(file_path))
        tmpdirs.append(tmpdir)
        logger.info(f'Extracting {file_path} to {tmpdir.name}')
        shutil.unpack_archive(filename=file_path, extract_dir=tmpdir.name)
        for root1, dirs1, files1 in os.walk(tmpdir.name):
            for file1 in files1:
                file_meta1 = copy.deepcopy(file_meta)
                file_meta1['folder_path'] = root1
                file_meta1['file_name'] = file1
                return process_pershing_file(file_meta=file_meta1)
    else:
        return process_pershing_file(file_meta=file_meta)



# %% Write list of tables to Azure

@catch_error(logger)
def write_table_list_to_azure(table_list:dict, firm_name:str, storage_account_name:str, storage_account_abbr:str):
    """
    Write all tables in table_list to Azure
    """
    global PARTITION_list

    if not table_list:
        logger.warning(f"No data to write")
        return

    for table_name, table in table_list.items():
        logger.info(f'Writing {table_name} to Azure...')

        add_table_to_tableinfo(
            tableinfo = tableinfo, 
            table = table, 
            schema_name = firm_name, 
            table_name = table_name, 
            tableinfo_source = tableinfo_source, 
            storage_account_name = storage_account_name,
            storage_account_abbr = storage_account_abbr,
            )

        container_folder = azure_container_folder_path(data_type=data_folder, domain_name=sys.domain_name, source_or_database=database, firm_or_schema=firm_name)

        if is_pc: # and manual_iteration:
            local_path = os.path.join(data_path_folder, 'temp') + fr'\{storage_account_name}\{container_folder}\{table_name}'
            pprint(fr'Save to local {local_path}')
            table.coalesce(1).write.json(path = fr'{local_path}.json', mode='overwrite')

        if save_pershing_to_adls_flag:
            userMetadata = save_adls_gen2(
                table = table,
                storage_account_name = storage_account_name,
                container_name = container_name,
                container_folder = container_folder,
                table_name = table_name,
                partitionBy = partitionBy,
                file_format = file_format
            )

            PARTITION_list[(sys.domain_name, database, firm_name, table_name, storage_account_name)] = userMetadata

    logger.info('Done writing to Azure')




# %% Process all files

@catch_error(logger)
def process_all_files():
    """
    Iterate over all the files in all the firms and process them.
    """
    all_new_files = None

    for firm in firms:
        folder_path = os.path.join(data_path_folder, firm['crd_number'])
        logger.info(f"Firm: {firm['firm_name']}, Firm CRD Number: {firm['crd_number']}")

        if not os.path.isdir(folder_path):
            logger.warning(f'Path does not exist: {folder_path}   -> SKIPPING')
            continue

        files_meta = get_all_pershing_file_meta(folder_path=folder_path, date_start=date_start, firm_crd_number=firm['crd_number'])
        if not files_meta:
            continue

        storage_account_name = to_storage_account_name(firm_name=firm['storage_account_abbr'])
        setup_spark_adls_gen2_connection(spark, storage_account_name)

        logger.info('Getting New Files')
        ingest_table = ingest_table_from_files_meta(files_meta, firm_name=firm['firm_name'], storage_account_name=storage_account_name, storage_account_abbr=firm['storage_account_abbr'])
        new_files, selected_files = get_selected_files(ingest_table)

        logger.info(f'Total of {new_files.count()} new file(s). {selected_files.count()} eligible for data migration.')

        if all_new_files:
            union_columns = new_files.columns
            all_new_files = all_new_files.select(union_columns).union(new_files.select(union_columns))
        else:
            all_new_files = new_files

        logger.info("Iterating over Selected Files")
        table_list_union = {}
        for ingest_row in selected_files.rdd.toLocalIterator():
            file_meta = ingest_row.asDict()

            table_list = process_one_file(file_meta=file_meta)

            if table_list:
                for table_name, table in table_list.items():
                    if table_name in table_list_union.keys():
                        table_prev = table_list_union[table_name]
                        primary_key_columns = [c for c in table_prev.columns if c.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()]]
                        if not primary_key_columns:
                            raise ValueError(f'No Primary Key Found for {table_name}')
                        table_prev = table_prev.alias('tp'
                            ).join(table, primary_key_columns, how='left_anti'
                            ).select('tp.*')
                        union_columns = table_prev.columns
                        table_prev = table_prev.select(union_columns).union(table.select(union_columns))
                        table_list_union[table_name] = table_prev.distinct()
                    else:
                        table_list_union[table_name] = table.distinct()

        write_table_list_to_azure(
            table_list = table_list_union,
            firm_name = firm['firm_name'],
            storage_account_name = storage_account_name,
            storage_account_abbr = firm['storage_account_abbr']
        )

        cleanup_tmpdirs()

    logger.info('Finished processing all Files and Firms')
    return all_new_files


all_new_files = process_all_files()



# %% Save Tableinfo metadata table into Azure and Save Ingest files metadata to SQL Server.

@catch_error(logger)
def save_tableinfo_dict_and_sql_ingest_table(tableinfo:defaultdict, all_new_files):
    """
    Save Tableinfo metadata table into Azure and Save Ingest files metadata to SQL Server.
    """
    if not tableinfo:
        logger.warning('No data in TableInfo --> Skipping write to Azure')
        return

    tableinfo_values = list(tableinfo.values())

    list_of_dict = []
    for vi in range(len(tableinfo_values[0])):
        list_of_dict.append({k:v[vi] for k, v in tableinfo.items()})

    meta_tableinfo = spark.createDataFrame(list_of_dict)
    meta_tableinfo = select_tableinfo_columns(tableinfo=meta_tableinfo)

    storage_account_name = default_storage_account_name # keep default storage account name for tableinfo
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    if save_tableinfo_adls_flag:
        save_adls_gen2(
                table = meta_tableinfo,
                storage_account_name = storage_account_name,
                container_name = tableinfo_container_name,
                container_folder = azure_container_folder_path(data_type=metadata_folder, domain_name=sys.domain_name, source_or_database=tableinfo_source),
                table_name = tableinfo_name,
                partitionBy = partitionBy,
                file_format = file_format,
            )

        if all_new_files: # Write ingest metadata to SQL so that the old files are not re-ingested next time.
            write_sql(
                table = all_new_files,
                table_name = sql_ingest_table_name,
                schema = sql_schema,
                database = sql_database,
                server = sql_server,
                user = sql_id,
                password = sql_pass,
                mode = 'append',
            )

    return meta_tableinfo



tableinfo = save_tableinfo_dict_and_sql_ingest_table(tableinfo=tableinfo, all_new_files=all_new_files)



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

