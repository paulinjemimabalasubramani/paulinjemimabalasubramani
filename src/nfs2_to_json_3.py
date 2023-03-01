description = """

Convert NFS2 fixed-width files to json format

"""


# %% Parse Arguments

if False: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_NFS_FSC',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\nfs2_schema\nfs2_schema.csv',
        'source_path': r'C:\myworkdir\data\NFS2_FSC',
        'db_name': 'ASSETS',
        'schema_name': 'NFS2',
        'clientid_map': 'MAJ:RAA,FXA:SPF,FL2:FSC,0KS:SAI,TR1:TRI,WDB:WFS',
        'file_history_start_date': '2023-01-15',
        'pipeline_firm': 'FSC',
        'add_firm_to_table_name': 'TRUE',
        'is_full_load': 'FALSE',
        }



# %% Import Libraries

import os, sys, tempfile, shutil, json, re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, get_csv_rows, normalize_name, convert_string_map_to_dict
from modules3.migrate_files import file_meta_exists_in_history

from collections import defaultdict
from distutils.dir_util import remove_tree
from datetime import datetime



# %% Parameters

HEADER_record = 'H'
DATA_record = 'D'
TRAILER_record = 'T'

json_file_ext = '.json'
json_file_date_format = r'%Y%m%d'

data_settings.target_path = data_settings.app_data_path
if os.path.isdir(data_settings.target_path): remove_tree(directory=data_settings.target_path, verbose=0, dry_run=0)
os.makedirs(data_settings.target_path, exist_ok=True)

headerrecordclientid_map = convert_string_map_to_dict(map_str=data_settings.clientid_map, uppercase_key=True, uppercase_val=True)



# %%

@catch_error(logger)
def clean_row(row):
    """
    Use cleaned up / normalized version of the row data
    """
    if not row:
        return {}

    clean_row = {
        'file_type': normalize_name(row['source_file_description']),
        'column_name': normalize_name(row['business_name']),
        'record_number': normalize_name(row['record_number']),
        'table_name': normalize_name(row['table_name']),
        'file_title': row['file_title'],
        'pos_start': int(row['source_field_start_pos']) - 1,
        'pos_end': int(row['source_field_end_pos']),
        'format': row['format'].strip(),
    }
    return clean_row



# %%

@catch_error(logger)
def get_nfs_schema():
    """
    Get Header file_title position information from schema
    """
    schema_file_path = data_settings.schema_file_path
    if not os.path.isfile(schema_file_path):
        raise RuntimeError(f'Schema file is not found: {schema_file_path}')

    file_titles = defaultdict(dict)
    for row in get_csv_rows(csv_file_path=schema_file_path):
        row1 = clean_row(row=row)

        if row1['file_title']:
            if file_titles[row1['file_type']]:
                raise RuntimeError(f'Scheme File Error: Found two file_title in row {row1} and in row ' + file_titles[row1['file_type']])
            if not row1['table_name']:
                raise RuntimeError(f'Table name is not found in row: {row}')
            file_titles[row1['file_type']] = row1

    all_schema = defaultdict(list)
    for row in get_csv_rows(csv_file_path=schema_file_path):
        row1 = clean_row(row=row)
        if row1['file_type'] not in file_titles:
            continue

        if row1['pos_end'] <= row1['pos_start']:
            raise RuntimeError(f'Scheme File Error: pos_end<pos_start in row {row}')

        if row1['column_name'] not in ['', 'not_used', 'filler', '_', '__', 'n_a', 'na', 'none', 'null']:
            for r in all_schema[(row1['file_type'], row1['record_number'])]:
                if r['column_name']==row1['column_name']:
                    raise RuntimeError(f'Scheme File Error: Duplicate column_name in row {row1} and in row {r}')
            all_schema[(row1['file_type'], row1['record_number'])].append(row1)

    return file_titles, all_schema



file_titles, all_schema = get_nfs_schema()



# %%

@catch_error(logger)
def convert_header_datetime(datetime_str:str, format:str):
    """
    Convert datetime string based on specific format to generic datetime object
    MMDDYY
    MMDDYYYY
    MM/DD/YYYY
    YYMMDD
    YYYYMMDD
    """
    f = format.upper().replace('YYYY', r'%Y').replace('YY', r'%y').replace('MM', r'%m').replace('DD', r'%d')
    return datetime.strptime(datetime_str, f)



# %%

@catch_error(logger)
def get_header_info(source_file_path):
    """
    Get file header info, and filter unwanted files.
    """
    with open(file=source_file_path, mode='rt') as f:
        HEADER = f.readline()

    header_info = None
    for file_type, row in file_titles.items():
        file_title = HEADER[row['pos_start']:row['pos_end']].strip().lower()
        if file_title == row['file_title']:
            header_schema = all_schema[(file_type, 'header')]

            header_info = {
                'database_name': data_settings.domain_name,
                'schema_name': data_settings.schema_name,
                'firm_name': data_settings.pipeline_firm.upper(),
                'table_name_no_firm': row['table_name'].lower(),
                'table_name': '_'.join([data_settings.pipeline_firm, row['table_name']]).lower(),
                'is_full_load': data_settings.is_full_load.upper() == 'TRUE',
            }

            header_info_format = dict()
            for h in header_schema:
                val = HEADER[h['pos_start']:h['pos_end']].strip()
                header_info = {**header_info, h['column_name']:val}
                header_info_format = {**header_info_format, h['column_name']:h['format']}

            for x in ['transmission_creation_date', 'run_date', 'header_date']:
                if header_info.get(x):
                    key_datetime = header_info[x]
                    key_datetime_format = header_info_format[x]
                    break

            header_info['key_datetime'] = convert_header_datetime(datetime_str=key_datetime, format=key_datetime_format)
            break

    return header_info


get_header_info(source_file_path = r'C:\myworkdir\data\NFS2_FSC\FSC_NFS_BOOK_S_230224.DAT')



# %%

@catch_error(logger)
def process_single_nfs2(source_file_path:str):
    """
    Process single nfs2 file
    """
    if not get_header_info(source_file_path=source_file_path):
        logger.warning(f'Unable to get header info for file: {source_file_path} -> skipping')
        return

    if file_meta_exists_in_history(file_meta=file_meta):
        logger.info(f'File already exists, skipping: {file_path}')
        return







# %%

@catch_error(logger)
def iterate_over_all_nfs2(source_path:str):
    """
    Main function to iterate over all the files in source_path and add bulk_id
    """
    for root, dirs, files in os.walk(source_path):
        for file_name in files:
            source_file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                    extract_dir = tmpdir
                    logger.info(f'Extracting {source_file_path} to {extract_dir}')
                    shutil.unpack_archive(filename=source_file_path, extract_dir=extract_dir, format='zip')
                    iterate_over_all_nfs2(source_path=extract_dir)
                continue

            if file_ext.lower() != '.dat':
                logger.warning(f'Not a .DAT file: {source_file_path}')
                continue

            process_single_nfs2(source_file_path=source_file_path)



iterate_over_all_nfs2(source_path=data_settings.source_path)






# %%



