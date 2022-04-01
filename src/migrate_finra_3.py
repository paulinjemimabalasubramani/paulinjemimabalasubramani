description = """
Flatten all Finra XML files and migrate them to ADLS Gen 2

https://brokercheck.finra.org/individual/summary/3132991

Official Finra Schemas:
https://www.finra.org/filing-reporting/web-crd/web-eft-schema-documentation-and-schema-files

"""


# %% Parse Arguments

if False: # Set to False for Debugging
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

import os, sys
from datetime import datetime

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, execution_date_start
from modules3.spark_functions import add_id_key, create_spark, read_csv, remove_column_spaces, add_elt_columns
from modules3.migrate_files import migrate_all_files, get_key_column_names, default_table_dtypes, file_meta_exists_for_select_files, add_firm_to_table_name
from modules3.build_finra_tables import build_branch_table, build_individual_table

from string import ascii_letters, digits



# %% Parameters

finra_individual_delta_name = 'IndividualInformationReportDelta'
reportDate_name = '_reportDate'
firmCRDNumber_name = '_firmCRDNumber'

DualRegistrations_file_name = 'INDIVIDUAL_-_DUAL_REGISTRATIONS_-_FIRMS_DOWNLOAD_-_'
DualRegistrations_name = 'DualRegistrations'

file_date_column_format = r'%Y-%m-%d'


# %% Create Connections

spark = create_spark()



# %% Extract metadata from finra file path

@catch_error(logger)
def extract_data_from_finra_file_path(file_path:str):
    """"
    Extract metadata from finra file path
    """
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)

    file_meta = {
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
        'firm_crd_number': data_settings.firm_crd_number,
    }

    if file_name_noext.upper().startswith(DualRegistrations_file_name.upper()):
        table_name = DualRegistrations_name.lower()
        file_date = execution_date_start
    else:
        sp = file_name_noext.split("_")

        if sp[0].strip() != data_settings.firm_crd_number:
            logger.warning(f'Wrong Firm CRD Number in file name. Expected {data_settings.firm_crd_number}. File: {file_path} -> SKIPPING')
            return

        try:
            table_name = sp[1].strip().lower()
            if table_name == finra_individual_delta_name.lower():
                table_name = 'IndividualInformationReport'.lower()

            file_date = datetime.strptime(sp[2].rsplit('.', 1)[0].strip(), file_date_column_format)

            assert len(sp)==3 or (len(sp)==4 and sp[1].strip().lower()==finra_individual_delta_name.lower())

        except Exception as e:
            logger.warning(f'Cannot parse file name: {file_path}. {str(e)}')
            return

    table_name = add_firm_to_table_name(table_name=table_name)

    file_meta = {**file_meta,
        'table_name': table_name.lower(),
        'file_date': file_date,
    }

    return file_meta



# %% Select Files

@catch_error(logger)
def select_files():
    """
    Initial Selection of candidate files potentially to be ingested
    """
    date_format = data_settings.date_format
    selected_file_paths = []

    file_count = 0
    for root, dirs, files in os.walk(data_settings.source_path):
        for file_name in files:
            file_count += 1
            file_path = os.path.join(root, file_name)

            file_meta = extract_data_from_finra_file_path(file_path=file_path)
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
    if file_name_noext.upper().startswith(DualRegistrations_file_name.upper()):
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
    file_meta = extract_data_from_finra_file_path(file_path=file_path)
    if not file_meta or file_meta['file_date'] < data_settings.key_datetime: return

    tagname, criteria_tag = get_xml_header(file_path=file_path)







    try:
        key_datetime = datetime.strptime(file_date_str, date_format)
    except Exception as e:
        logger.warning(f'Invalid date format in file name: {file_path}. {str(e)}')
        return

    file_meta = {**file_meta,
        'zip_file_path': zip_file_path,
        'is_full_load': data_settings.is_full_load.upper() == 'TRUE',
        'key_datetime': key_datetime,
    }

    return file_meta




# %% Main Processing of sinlge csv File

@catch_error(logger)
def process_csv_file(file_meta):
    """
    Main Processing of single csv file
    """
    table = read_csv(spark=spark, file_path=file_meta['file_path'])
    if not table: return

    table = remove_column_spaces(table=table)

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

additional_file_meta_columns = []

migrate_all_files(
    spark = spark,
    fn_extract_file_meta = extract_csv_file_meta,
    additional_file_meta_columns = additional_file_meta_columns,
    fn_process_file = process_csv_file,
    fn_select_files = select_files,
    fn_get_dtypes = get_dtypes,
    )



# %% Close Connections / End Program

mark_execution_end()


# %%


