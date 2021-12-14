"""
Read all ASSETS - FRONTPOINT files and migrate to the ADLS Gen 2

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""


# %% Parse Arguments

if False: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description='Migrate any CSV type files with date info in file name')

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_FRONTPOINT_SAI',
        'source_path': r'C:\packages\Shared\FRONTPOINT',
        'schema_file_path': r'C:\packages\EDIP-Code\config\assets\frontpoint_schema\frontpoint_schema.csv'
        }



# %% Import Libraries

import os, sys, csv
sys.args = args
sys.parent_name = os.path.basename(__file__)

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules2.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, get_pipeline_info
from modules2.spark_functions import IDKeyIndicator, add_md5_key, create_spark, read_csv, remove_column_spaces, add_elt_columns
from modules2.azure_functions import read_tableinfo_rows, tableinfo_name
from modules2.snowflake_ddl import connect_to_snowflake, iterate_over_all_tables_snowflake, create_source_level_tables, snowflake_ddl_params
from modules2.migrate_files import save_tableinfo_dict_and_cloud_file_history, process_all_files_with_incrementals, get_key_column_names

from datetime import datetime
from collections import defaultdict

from pyspark import StorageLevel
from pyspark.sql.functions import col, lit, to_date, to_timestamp



# %% Parameters

tableinfo_source = data_settings.schema_name
date_start = datetime.strptime(data_settings.file_history_start_date, r'%Y-%m-%d')

date_column_name = 'run_datetime'
key_column_names = get_key_column_names(date_column_name=date_column_name)
unused_column_name = 'unused'

pipeline_info = get_pipeline_info(pipelinekey=data_settings.pipelinekey)
firm_name = '' # pipeline_info['firm'].upper() # No Single Firm Name is used

logger.info({
    'schema_name': data_settings.schema_name,
    'source_path': data_settings.source_path,
    **pipeline_info,
})



# %% Create Spark Session

spark = create_spark()
snowflake_ddl_params.spark = spark


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



# %% Extract Meta Data from Frontpoint file

@catch_error(logger)
def extract_frontpoint_file_meta(file_path:str, cloud_file_history):
    """
    Extract Meta Data from Frontpoint file name
    """
    strftime = r'%Y-%m-%d %H:%M:%S' # http://strftime.org/

    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()
    file_name_list = file_name_noext.split('_')

    if not len(file_name_list) in [5, 6]:
        logger.warning(f'Cannot parse Frontpoint file name: {file_path}')
        return

    is_test = False
    if len(file_name_list) == 6:
        if file_name_list[2].lower() != 'test':
            logger.warning(f'Not a test file: {file_path}')
            return
        is_test = True
        file_name_list.pop(2)

    table_name = "_".join([file_name_list[0], file_name_list[1]]).lower()
    if table_name not in file_schema:
        logger.warning(f'Table name should be one of {list(file_schema)} for file: {file_path}')
        return

    month = file_name_list[2]
    day = file_name_list[3]
    if not month.isdigit() or len(month)!=2 or not day.isdigit() or len(day)!=2:
        logger.warning(f'Invalid Month/Day for file: {file_path}')
        return
    now = datetime.now()
    file_date_last_yr = datetime.strptime('-'.join([str(now.year-1), month, day]), r'%Y-%m-%d')
    file_date_curr_yr = datetime.strptime('-'.join([str(now.year), month, day]), r'%Y-%m-%d')
    file_date_next_yr = datetime.strptime('-'.join([str(now.year+1), month, day]), r'%Y-%m-%d')

    if (file_date_next_yr - now).days < 5:
        file_date = file_date_next_yr
    elif abs((file_date_curr_yr - now).days) > abs((file_date_last_yr - now).days):
        file_date = file_date_last_yr
    else:
        file_date = file_date_curr_yr

    is_full_load = file_name_list[3].lower() == 'full'



    sequence_number = file_name_noext[-2:]
    file_date = convert_yyyymmdd(file_name_noext[-10:-2])
    file_name_prefix = file_name_noext[:-10]

    fin_inst_id = crd_to_fid.get(firm_crd_number, None)
    if not fin_inst_id:
        logger.warning(f'Unknown CRD Number / No equivalent Financial Institution ID found: {firm_crd_number}')
        return

    if len(file_name_prefix)<=len(fin_inst_id) or file_name_prefix[-len(fin_inst_id):].upper()!=fin_inst_id.upper():
        logger.warning(f'Financial Institution ID is incorrect. Found {file_name_prefix[-len(fin_inst_id):]}  Expected: {fin_inst_id}')
        return

    file_type = file_name_prefix[:-len(fin_inst_id)].upper()
    if file_type not in albridge_file_types:
        logger.warning(f'Unknown Albridge file type: {file_type}')
        return

    file_meta = {
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        FirmCRDNumber: firm_crd_number,
        'firm_name': fin_inst_ids[fin_inst_id],
        'sequence_number': sequence_number,
        'file_date': file_date,
        'file_type': file_type,
        'file_type_desc': albridge_file_types[file_type],
        'fin_inst_id': fin_inst_id,
    }

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

    for i in range(1, len(header_schema)):
        file_meta[header_schema[i]] = HEADER[i]

    file_meta['table_name'] = file_meta['file_desc'][:-4].lower()
    file_meta['eff_date'] = convert_yyyymmdd(file_meta['eff_date'])
    file_meta['run_date'] = convert_yyyymmdd(file_meta['run_date'])
    file_meta['run_time'] = convert_hhmmss(file_meta['run_time'])

    file_meta['is_full_load'] = False # determine how to know if the Albridge file is full load or not.
    file_meta[date_column_name] = datetime.strptime(' '.join([file_meta['run_date'], file_meta['run_time']]), strftime)
    return file_meta



# %% Create table from given Albridge file and its schema

@catch_error(logger)
def create_table_from_albridge_file(file_path:str, file_schema):
    text_file = read_text(spark=spark, file_path=file_path)

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
    text_file = add_id_key(text_file, key_column_names=key_column_names)

    text_file.persist(StorageLevel.MEMORY_AND_DISK)
    return text_file



# %% Main Processing of an Albridge File

@catch_error(logger)
def process_albridge_file(file_meta, cloud_file_history):
    """
    Main Processing of single Albridge file
    """
    logger.info(file_meta)

    file_path = os.path.join(file_meta['folder_path'], file_meta['file_name'])
    file_schema = schema[file_meta['file_type']]

    table = create_table_from_albridge_file(
        file_path = file_path,
        file_schema = file_schema,
        )
    if not table: return

    table = remove_column_spaces(table=table)
    table = table.withColumn(date_column_name, lit(str(file_meta[date_column_name])))
    table = table.withColumn(FirmCRDNumber, lit(str(file_meta[FirmCRDNumber])))

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
    to_date(col('file_date'), format='yyyy-MM-dd').alias('file_date', metadata={'sqltype': '[date] NULL'}),
    to_date(col('eff_date'), format='yyyy-MM-dd').alias('eff_date', metadata={'sqltype': '[date] NULL'}),
    col('file_type').cast(StringType()).alias('file_type', metadata={'maxlength': 10, 'sqltype': 'varchar(10)'}),
    col('file_type_desc').cast(StringType()).alias('file_type_desc', metadata={'maxlength': 30, 'sqltype': 'varchar(30)'}),
    col('sequence_number').cast(StringType()).alias('sequence_number', metadata={'maxlength': 10, 'sqltype': 'varchar(10)'}),
    col('fin_inst_id').cast(StringType()).alias('fin_inst_id', metadata={'maxlength': 10, 'sqltype': 'varchar(10)'}),
    ]

all_new_files, PARTITION_list, tableinfo = process_all_files_with_incrementals(
    spark = spark,
    firms = firms,
    data_path_folder = data_path_folder,
    fn_extract_file_meta = extract_albridge_file_meta,
    date_start = date_start,
    additional_ingest_columns = additional_ingest_columns,
    fn_process_file = process_albridge_file,
    key_column_names = key_column_names,
    tableinfo_source = tableinfo_source,
    save_data_to_adls_flag = save_albridge_to_adls_flag,
    date_column_name = date_column_name,
    use_crd_number_as_folder_name = False,
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

