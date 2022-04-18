description = """
Flatten all Finra XML files and migrate them to ADLS Gen 2

https://brokercheck.finra.org/individual/summary/3132991

Official Finra Schemas:
https://www.finra.org/filing-reporting/web-crd/web-eft-schema-documentation-and-schema-files

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
        'pipelinekey': 'FP_MIGRATE_FINRA_RAA',
        'source_path': r'C:\myworkdir\Shared\FINRA\23131',
        }



# %% Import Libraries

import os, sys, json
from datetime import datetime

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc
from modules3.spark_functions import add_id_key, create_spark, remove_column_spaces, add_elt_columns, read_csv, read_xml, base_to_schema, flatten_table
from modules3.migrate_files import migrate_all_files, get_key_column_names, default_table_dtypes, file_meta_exists_for_select_files
from modules3.build_finra_tables import build_branch_table, build_individual_table
from modules3.finra_header import extract_data_from_finra_file_path, finra_dualregistrations_file_name, finra_branch_name, finra_dualregistrations_name, file_date_column_format, finra_individual_name

from pyspark.sql.functions import lit, from_json
from pyspark.sql.types import StructType, StructField, StringType



# %% Parameters



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

            file_meta = extract_data_from_finra_file_path(file_path=file_path, get_firm_crd=True)
            if not file_meta or file_meta['file_date'] < data_settings.key_datetime: continue

            if file_meta_exists_for_select_files(file_path=file_path): continue

            selected_file_paths.append((file_path, file_meta['file_date']))

    selected_file_paths = sorted(selected_file_paths, key=lambda c: (c[1], c[0]))
    selected_file_paths = [c[0] for c in selected_file_paths]
    return file_count, selected_file_paths



# %% Get XML Header

@catch_error(logger)
def get_xml_header(file_path:str):
    """
    Function to read header info from inside the XML file. This is to speed up the ingestion process - to read XML files meta without Spark
    """
    fulltag, quoteon, tagname, valstr, keystr, tag, criteria_tag = False, False, '', '', '', {}, {}

    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    if file_name_noext.upper().startswith(finra_dualregistrations_file_name.upper()):
        return tagname, criteria_tag

    nchar = 1000
    break2 = False
    with open(file=file_path, mode='rt', encoding='ISO-8859-1', errors='ignore') as f:
        while True:
            if break2: break
            schar = f.read(nchar)

            for char in schar:
                if char in ['\n', '\r']: char = ' '

                if char=='>' and not quoteon:
                    if tagname.lower() == 'Criteria'.lower():
                        criteria_tag = tag
                    elif criteria_tag and tagname.lower() != '/Criteria'.lower():
                        break2 = True
                        break
                    fulltag = False
                    tagname, keystr, valstr, tag = '', '', '', {}
                elif char == '"':
                    quoteon = not quoteon
                    if not quoteon and keystr:
                        tag[keystr] = valstr
                        keystr, valstr = '', ''
                elif quoteon: valstr += char
                elif not fulltag:
                    if char == '<': continue
                    if char == ' ' and tagname:
                        fulltag = True
                        continue
                    if char != ' ' or tagname: tagname += char
                elif char not in [' ', '=']:
                    keystr += char

            if len(schar) < nchar: break

    return tagname, criteria_tag



# %% Extract Meta Data from finra file

@catch_error(logger)
def extract_finra_file_meta(file_path:str, zip_file_path:str=None):
    """
    Extract Meta Data from finra file
    """
    file_meta = extract_data_from_finra_file_path(file_path=file_path, get_firm_crd=True)
    if not file_meta or file_meta['file_date'] < data_settings.key_datetime: return

    tagname, criteria_tag = get_xml_header(file_path=file_path)

    if file_meta['table_name_no_firm'].lower() in [finra_dualregistrations_name.lower()]:
        key_datetime = file_meta['file_date']
    elif criteria_tag:
        if criteria_tag.get('firmCRDNumber') != data_settings.firm_crd_number:
            logger.warning(f"File Firm CRD Number {criteria_tag.get('firmCRDNumber')} does not match with the CRD Number {data_settings.firm_crd_number} given in SQL Settings")
            return
        try:
            key_datetime = criteria_tag['reportDate'] if criteria_tag.get('reportDate') else criteria_tag['postingDate']
            key_datetime = datetime.strptime(key_datetime, file_date_column_format)
        except Exception as e:
            logger.warning(f'Invalid date format in file name: {file_path}. {str(e)}')
            return
    else:
        logger.warning(f'No xml_criteria found: {file_path}')
        return

    is_full_load = criteria_tag.get('IIRType') == 'FULL' or file_meta['table_name_no_firm'].lower() in [finra_branch_name.lower(), finra_dualregistrations_name.lower()]

    file_meta = {**file_meta,
        'zip_file_path': zip_file_path,
        'is_full_load': is_full_load,
        'key_datetime': key_datetime,
        'xml_criteria': criteria_tag,
        'xml_rowtag': tagname,
    }

    return file_meta



# %% Read Finra File

@catch_error(logger)
def read_finra_file(file_meta:dict):
    """
    Read Finra File
    """
    if file_meta['table_name_no_firm'].lower() in [finra_dualregistrations_name.lower()]:
        table = read_csv(spark=spark, file_path=file_meta['file_path'])
    else:
        schema_file = file_meta['table_name_no_firm'].lower() + '.json'
        schema_path = os.path.join(data_settings.schema_file_path, schema_file)

        if os.path.isfile(schema_path):
            logger.info(f'Loading schema from file: {schema_file}')
            with open(file=schema_path, mode='rt', encoding='utf-8', errors='ignore') as f: 
                schema = base_to_schema(json.load(f))
        else:
            logger.warning(f"No manual schema defined for {file_meta['table_name_no_firm'].lower()} at Schema File Location {schema_path} -> Inferring schema from data.")
            schema = None

        table = read_xml(spark=spark, file_path=file_meta['file_path'], rowTag=file_meta['xml_rowtag'], schema=schema)

    if is_pc: table.printSchema()
    return table



# %% Flatten Finra Table

@catch_error(logger)
def flatten_finra_table(table, file_meta:dict):
    """
    Flatten Finra Table
    """
    semi_flat_table = flatten_table(table=table)

    if file_meta['table_name_no_firm'].lower() == finra_branch_name.lower():
        semi_flat_table = build_branch_table(semi_flat_table=semi_flat_table)
    elif file_meta['table_name_no_firm'].lower() == finra_individual_name.lower():
        semi_flat_table = build_individual_table(semi_flat_table=semi_flat_table, crd_number=file_meta['firm_crd_number'])

    return semi_flat_table



# %% Add custom columns to Finra table

@catch_error(logger)
def add_custom_columns_to_finra_table(table, file_meta:dict):
    """
    Add custom columns to Finra table
    """
    table = table.withColumn('file_date', lit(str(file_meta['file_date'])))
    table = table.withColumn('firm_crd_number', lit(str(file_meta['firm_crd_number'])))

    if not file_meta['xml_criteria']: return table

    xml_criteria_str = json.dumps(file_meta['xml_criteria'], ensure_ascii=True, skipkeys=True)
    xml_criteria_schema = StructType([StructField(key, StringType(), True) for key in file_meta['xml_criteria']])

    table = table.withColumn('xml_criteria', from_json(lit(xml_criteria_str), xml_criteria_schema))
    return table



# %% Main Processing of sinlge finra File

@catch_error(logger)
def process_finra_file(file_meta:dict):
    """
    Main Processing of single finra file
    """
    table = read_finra_file(file_meta=file_meta)
    if not table: return

    table = flatten_finra_table(table=table, file_meta=file_meta)
    table = remove_column_spaces(table=table)
    table = add_custom_columns_to_finra_table(table=table, file_meta=file_meta)

    key_column_names = get_key_column_names(table_name=file_meta['table_name'])
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

additional_file_meta_columns = [
    ('file_date', 'datetime NULL'),
    ('xml_rowtag', 'varchar(100) NULL'),
    ('xml_criteria', 'varchar(2000) NULL'),
    ]

migrate_all_files(
    spark = spark,
    fn_extract_file_meta = extract_finra_file_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_finra_file,
    fn_select_files = select_files,
    fn_get_dtypes = get_dtypes,
    )



# %% Close Connections / End Program

mark_execution_end()


# %%


