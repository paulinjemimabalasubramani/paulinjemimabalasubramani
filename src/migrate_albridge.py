"""
Read all Albridge files and migrate to the ADLS Gen 2

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""


# %% Import Libraries

import os, sys, re, csv
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'customer_assets'
sys.domain_abbr = 'ASSETS'


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

tableinfo_source = 'ALBRIDGE'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))
schema_folder_path = os.path.join(config_path, 'albridge_schema')
schema_file_name = 'albridge_schema.csv'

date_start = datetime.strptime(data_settings.file_history_start_date[tableinfo_source], r'%Y-%m-%d')

schema_header_str = 'HEADER'
schema_trailer_str = 'TRAILER'

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

# temporarily change firm crd number to firm name:
for i in range(len(firms)):
    firms[i]['crd_number'] = firms[i]['firm_name']



# %% get and pre-process schema

@catch_error(logger)
def get_albridge_schema():
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    schema_file_path = os.path.join(schema_folder_path, schema_file_name)

    schema = defaultdict(list)
    albridge_file_types = dict()

    with open(schema_file_path, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            file_types = row['file_type'].upper().split(',')
            is_primary_key = row['is_primary_key'].strip().upper() == 'Y'

            column_name = row['column_name'].strip().lower()
            if column_name in ['', 'n/a', 'none', '_', '__', 'na', 'null', '-', '.']:
                column_name = 'unused'

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
def extract_albridge_file_meta(file_path:str, firm_crd_number:str, cloud_file_history):
    """
    Extract Meta Data from Albridge file (reading 1st line (header metadata) from inside the file)
    """
    convert_yyyymmdd = lambda s: f'{s[:4]}-{s[4:6]}-{s[6:]}'
    convert_hhmmss = lambda s: f'{s[:2]}:{s[2:4]}:{s[4:]}'
    strftime = r'%Y-%m-%d %H:%M:%S' # http://strftime.org/

    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.upper()

    if file_ext.lower() not in ['.txt']:
        logger.warning(f'Not a .txt file: {file_path}')
        return

    if not 13<=len(file_name_noext)<=16 \
        or not file_name_noext[0].isalpha() \
        or not file_name_noext[2:].isdigit() \
        or not (file_name_noext[:1] in albridge_file_types or file_name_noext[:2] in albridge_file_types):
        logger.warning(f'Not a valid Albirdge Replication/Export file name: {file_path}')
        return

    sequence_number = file_name_noext[-2:]
    file_date = convert_yyyymmdd(file_name_noext[-10:-2])
    file_name_prefix = file_name_noext[:-10]

    if len(file_name_prefix)==3:
        if file_name_prefix[:1] in albridge_file_types and file_name_prefix[1:].isdigit():
            file_type = file_name_prefix[:1]
            fin_inst_id = file_name_prefix[1:]
        elif file_name_prefix[:2] in albridge_file_types and file_name_prefix[2:].isdigit():
            file_type = file_name_prefix[:2]
            fin_inst_id = file_name_prefix[2:]
        else:
            logger.warning(f'Not a valid Albirdge Replication/Export file name prefix: {file_path}')
            return
    else:
        if file_name_prefix[:2] in albridge_file_types and file_name_prefix[2:].isdigit():
            file_type = file_name_prefix[:2]
            fin_inst_id = file_name_prefix[2:]
        elif file_name_prefix[:1] in albridge_file_types and file_name_prefix[1:].isdigit():
            file_type = file_name_prefix[:1]
            fin_inst_id = file_name_prefix[1:]
        else:
            logger.warning(f'Not a valid Albirdge Replication/Export file name prefix: {file_path}')
            return

    file_meta = {
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        FirmCRDNumber: firm_crd_number,
        'sequence_number': sequence_number,
        'file_date': file_date,
        'file_type': file_type,
        'file_type_desc': albridge_file_types[file_type],
        'fin_inst_id': fin_inst_id,
    }

    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    if HEADER[:2] != 'H|':
        logger.warning(f'Header is not found in 1st line inside the file: {file_path}')
        return

    HEADER = HEADER.split(sep='|')
    header_schema = [c['column_name'].lower() for c in schema['HEADER']]

    for i in range(1, len(header_schema)):
        file_meta[header_schema[i]] = HEADER[i]

    file_meta['table_name'] = file_meta['file_desc'][:-4]
    file_meta['eff_date'] = convert_yyyymmdd(file_meta['eff_date'])
    file_meta['run_date'] = convert_yyyymmdd(file_meta['run_date'])
    file_meta['run_time'] = convert_hhmmss(file_meta['run_time'])

    file_meta['is_full_load'] = None
    file_meta[date_column_name] = datetime.strptime(' '.join([file_meta['run_date'], file_meta['run_time']]), strftime)
    return file_meta




file_path = r'C:\Users\smammadov\packages\Shared\ALBRIDGE\WFS\A632021111001.TXT'
firm_crd_number = 'WFS'
cloud_file_history = ''

file_meta = extract_albridge_file_meta(file_path, firm_crd_number, cloud_file_history)



# %% Main Processing of Pershing File

@catch_error(logger)
def process_pershing_file(file_meta, cloud_file_history):
    """
    Main Processing of single Pershing file
    """
    file_path = os.path.join(file_meta['folder_path'], file_meta['file_name'])

    logger.info(file_meta)

    firm_crd_number = file_meta[FirmCRDNumber]
    file_name = file_meta['file_name']

    firm_path_folder = os.path.join(data_path_folder, firm_crd_number)
    file_path = os.path.join(firm_path_folder, file_name)

    file_meta = extract_pershing_file_meta(file_path=file_path, firm_crd_number=firm_crd_number, cloud_file_history=cloud_file_history)
    table_name = file_meta['table_name']

    table = create_table_from_fwt_file(
        firm_path_folder = os.path.join(data_path_folder, firm_crd_number),
        file_name = file_name,
        schema_file_name = file_meta['schema_file_name'],
        table_name = table_name,
        groupBy=groupBy,
    )
    if not table: return

    table = remove_column_spaces(table_to_remove = table)
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

