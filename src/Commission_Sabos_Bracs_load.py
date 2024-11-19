description = """

Read all SABOS and Bracs files and migrate to the ADLS Gen 2

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
        'pipelinekey': 'Commission_Sabos',
        'source_path': r'C:\myworkdir\Shared\SABOS',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\Sabos\Sabos_Schema.csv',
        }



# %% Import Libraries

import os, sys
from datetime import datetime
from collections import defaultdict

import re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, get_csv_rows
from modules3.spark_functions import add_id_key, create_spark, read_text, remove_column_spaces, add_elt_columns
from modules3.migrate_files import migrate_all_files, default_table_dtypes, file_meta_exists_for_select_files, add_firm_to_table_name

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import ArrayType, StringType




# %% Parameters

allowed_file_extensions = ['.txt']

unused_column_name = 'unused'

master_schema_header_columns = {
    'file_date': 'datetime NULL',
    'bdid': 'int 0'
    }

# BDID Mappings based on file prefixes
BDID_MAPPING = {
    'OAS': 7,  # BRACS
    'RAA': 1   # SABOS
}

# Utility Functions
def get_bdid(zip_file_name):
    """Determine BDID based on the zip file name prefix."""
    prefix = zip_file_name[:3]
    return BDID_MAPPING.get(prefix, None)

def extract_file_date(header_line):
    """Extract the FileDate from the header line of a file."""
    return header_line.split(",")[1].strip() if "*BOF*" in header_line else None


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
                    file_date_str = file_name_noext[-8:]

                key_datetime = datetime.strptime(file_date_str, data_settings.date_format)
                if key_datetime < data_settings.key_datetime: continue
            except Exception as e:
                logger.warning(f'Invalid Date Format: {file_path}. {str(e)}')
                continue

            if file_meta_exists_for_select_files(file_path=file_path): continue

            selected_file_paths.append((file_path, key_datetime))

    selected_file_paths = sorted(selected_file_paths, key=lambda c: (c[1], c[0]))
    selected_file_paths = [c[0] for c in selected_file_paths]
    return file_count, selected_file_paths



# %% get and pre-process schema

@catch_error(logger)
def get_schema():
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    schema = defaultdict(list)
    SB_file_types = dict()

    for row in get_csv_rows(csv_file_path=data_settings.schema_file_path):
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
            
            if ftype not in SB_file_types:
                SB_file_types[ftype] = file_type_desc

    return schema,SB_file_types


schema, SB_file_types  = get_schema()


# %% Extract Meta Data from SABOS/BRACS file

@catch_error(logger)
def extract_file_meta(file_path:str, zip_file_path:str=None):
    """
    Extract Meta Data from SABOS/BRACS file (reading 1st line (header metadata) from inside the file)
    """
    zip_file_name = os.path.basename(zip_file_path) if zip_file_path else None
    zip_file_name_noext, zip_file_ext = os.path.splitext(zip_file_name) if zip_file_name else (None, None)
    bdid = get_bdid(zip_file_name_noext) if zip_file_name else None

    if bdid is None and zip_file_name:
       logger.warning(f"Skipping unrecognized zip file: {zip_file_name}")
       return

    # Process regular file name
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)

    if zip_file_name is None:
       bdid = get_bdid(file_name_noext)
    else:
       bdid = get_bdid(zip_file_name_noext)

    if bdid is None and zip_file_name is None:
       logger.warning(f"Skipping unrecognized file: {file_name}")
       return
    

    with open(file_path, 'rb') as f:
        first_line = f.readline().decode("utf-8")
    file_date = extract_file_date(first_line)

    file_type = file_name_noext
    table_name = SB_file_types[file_type].lower()

    date_str = zip_file_name_noext[-8:] if zip_file_name else None
    date_str = file_name_noext[-8:]
    key_datetime = datetime.strptime(date_str, "%Y%m%d")

    file_meta = {
        'table_name': table_name,
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'zip_file_path': zip_file_path,
        'zip_file': zip_file_name,
        'is_full_load': False,
        'key_datetime': key_datetime,

        'file_date': file_date,
        'bdid': bdid,
        'file_type_desc': SB_file_types[file_type],
        'file_type': file_type
        }                

    return file_meta 


def parse_line(line):
    pattern = re.compile(r'''((?:[^,"']|"[^"]*"|'[^']*')+)''')
    return [x.strip() for x in pattern.split(line) if x.strip() and x != ',']


# %% Create table from given file and its schema

@catch_error(logger)
def create_table_from_file(file_meta:dict):
    file_schema = schema[file_meta['file_type']]
    text_file = read_text(spark=spark, file_path=file_meta['file_path'])
    if not text_file: return None, None

    # Register the function as a UDF
    parse_line_udf = udf(parse_line, ArrayType(StringType()))

    # Apply the UDF to the DataFrame
    text_file = (text_file
    .where(col('value').substr(0, 5) != lit('*BOF*'))  # Exclude the BOF line
    .where(col('value').substr(0, 5) != lit('*EOF*'))  # Exclude the EOF line
    .withColumn('elt_value', parse_line_udf(col('value')))
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



# %% Main Processing of a File

@catch_error(logger)
def process_file(file_meta:dict):

    """Main Processing of each file"""

    table, key_column_names = create_table_from_file(file_meta=file_meta)
    if not table: return

    table = remove_column_spaces(table=table)
    table = add_id_key(table=table, key_column_names=key_column_names)

    dml_type = 'I' if file_meta['is_full_load'] else 'U'
    table = add_elt_columns(table=table, file_meta=file_meta, dml_type=dml_type)

    if is_pc: table.show(5)

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

migrate_all_files(
    spark = spark,
    fn_extract_file_meta = extract_file_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_file,
    fn_select_files = select_files,
    fn_get_dtypes = get_dtypes,
    )



# %% Close Connections / End Program

mark_execution_end()


# %%