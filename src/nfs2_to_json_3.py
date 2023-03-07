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
        'clientid_map': 'MAJ:RAA,FXA:SPF,FL2:FSC,00133:SAI,00436:TRI,WDB:WFS,0KS:SAI,TR1:TRI',
        'file_history_start_date': '2023-01-15',
        'pipeline_firm': 'FSC',
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

    pic = row['source_field_pic'].upper().replace('PIC','').replace(' ','').split('V')
    if len(pic)<=1:
        decimals = 0
    else:
        d = pic[-1]
        if any(x.isalpha() for x in d):
            decimals = 0
        elif '(' in d:
            dx = d.split('(')
            dx = dx[1].split(')')
            decimals = int(dx[0])
        else:
            decimals = d.count('9')

    clean_row = {
        'file_type': normalize_name(row['source_file_description']),
        'column_name': normalize_name(row['business_name']),
        'record_number': normalize_name(row['record_number']),
        'table_name': normalize_name(row['table_name']),
        'file_title': row['file_title'].strip().lower(),
        'pos_start': int(row['source_field_start_pos'].strip()) - 1,
        'pos_end': int(row['source_field_end_pos'].strip()),
        'format': row['format'].strip(),
        'decimals': decimals,
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
def extract_nfs2_file_meta(file_path:str, zip_file_path:str=None):
    """
    Get file header info, and filter unwanted files.
    """
    file_name = os.path.basename(file_path)

    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    file_meta = None
    for file_type, row in file_titles.items():
        file_title = HEADER[row['pos_start']:row['pos_end']].strip().lower()
        if file_title == row['file_title']:
            header_schema = all_schema[(file_type, 'header')]

            table_suffix = ''
            client_id = ''
            if row['table_name'] in ['bookkeeping', 'account_balance']:
                client_id = HEADER[1:6].strip() # get client id for IWS files (different from NFS headers).
                if len(client_id)>3:
                    table_suffix = '_iws'

            file_meta = {
                'database_name': data_settings.domain_name,
                'schema_name': data_settings.schema_name,
                'firm_name': data_settings.pipeline_firm.upper(),
                'table_name_no_firm': row['table_name'].lower() + table_suffix,
                'table_name': '_'.join([data_settings.pipeline_firm, row['table_name']]).lower() + table_suffix,
                'file_type': row['file_type'],
                'is_full_load': data_settings.is_full_load.upper() == 'TRUE',
                'file_name': file_name,
                'file_path': file_path,
                'folder_path': os.path.dirname(file_path),
                'zip_file_path': zip_file_path,
            }

            header_info_format = dict()
            for h in header_schema:
                val = HEADER[h['pos_start']:h['pos_end']].strip()
                file_meta = {**file_meta, h['column_name']:val}
                header_info_format = {**header_info_format, h['column_name']:h['format']}

            if client_id: # overwirte client_id for select files
                file_meta['header_record_client_id'] = client_id

            for x in ['transmission_creation_date', 'run_date', 'header_date']:
                if file_meta.get(x):
                    key_datetime = file_meta[x]
                    key_datetime_format = header_info_format[x]
                    break

            file_name_noext = os.path.splitext(os.path.basename(file_path))[0].replace('.', '_')
            file_meta['key_datetime'] = convert_header_datetime(datetime_str=key_datetime, format=key_datetime_format)
            file_meta['json_file_path'] = os.path.join(data_settings.target_path, file_name_noext
                                                       + '.' + file_meta['table_name']
                                                       + '_' + file_meta['key_datetime'].strftime(json_file_date_format)
                                                       + json_file_ext)
            break

    return file_meta



# %%

@catch_error(logger)
def new_record(file_meta:dict):
    """
    Create new record with basic info
    """
    record = {
        'header_firm_name': file_meta['firm_name'],
    }

    if 'header_record_client_id' in file_meta:
        record['header_client_id'] = file_meta['header_record_client_id']

    return record



# %%

def extract_values_from_line(line:str, record_schema:list):
    """
    Extract all values from single line string based on its schema
    """
    field_values = dict()
    for field in record_schema:
        field_value = line[field['pos_start']:field['pos_end']]
        field_value = re.sub(' +', ' ', field_value.strip())

        if field['decimals']>0 and field_value:
            if not field_value.isdigit():
                raise ValueError(f'Schema Scale mismatch for field "{field["column_name"]}" field value "{field_value}". Field Value should be all digits!')
            x = len(field_value) - field['decimals']
            if x<0:
                raise ValueError(f'Length of number {field_value} is less than the decimal number {field["decimals"]} for field "{field["column_name"]}"')
            field_value = float(field_value[:x] + '.' + field_value[x:])

        field_values[field['column_name']] = field_value

    return field_values



# %%

@catch_error(logger)
def process_lines_bookkeeping(fsource, ftarget, file_meta:dict):
    """
    Process all lines for bookkeeping table
    """
    record_schema = all_schema[(file_meta['file_type'], 'record')]

    first = True
    for line in fsource:
        if line[0]!='D':
            continue

        record = new_record(file_meta=file_meta)
        record = {**record, **extract_values_from_line(line=line, record_schema=record_schema)}

        if not first:
            ftarget.write(',\n')

        ftarget.write(json.dumps(record))
        first = False

    return not first



# %%

process_lines_map = {
    'bookkeeping': process_lines_bookkeeping,
    'bookkeeping_iws': process_lines_bookkeeping,
    'account_balance': process_lines_bookkeeping,
    'account_balance_iws': process_lines_bookkeeping,
}



# %%

@catch_error(logger)
def convert_nfs2_to_json(file_meta:dict):
    """
    Convert given NFS2 DAT file to Json format.
    """
    source_file_path = file_meta['file_path']
    target_file_path = file_meta['json_file_path']

    logger.info(f'Converting to JSON: {source_file_path}')
    with open(source_file_path, mode='rt', encoding='ISO-8859-1', errors='ignore') as fsource:
        with open(target_file_path, mode='wt', encoding='utf-8') as ftarget:
            ftarget.write('[\n')
            data_exists = process_lines_map[file_meta['table_name_no_firm']](fsource=fsource, ftarget=ftarget, file_meta=file_meta)
            ftarget.write('\n]')

    if not data_exists:
        logger.warning(f'No Data in the file {source_file_path} -> Skipping conversion to JSON and deleting {target_file_path}.')
        os.remove(target_file_path)
        return

    logger.info(f'Finished converting file {source_file_path} to JSON file {target_file_path}')



# %%

@catch_error(logger)
def process_single_nfs2(file_path:str):
    """
    Process single nfs2 file
    """
    file_meta = extract_nfs2_file_meta(file_path=file_path)
    if not file_meta:
        logger.warning(f'Unable to get header info for file: {file_path} -> skipping')
        return

    if file_meta_exists_in_history(file_meta=file_meta):
        logger.info(f'File already exists, skipping: {file_path}')
        return

    if data_settings.key_datetime > file_meta['key_datetime']:
        logger.info(f'File datetime {file_meta["key_datetime"]} is older than the datetime threshold {data_settings.key_datetime}, skipping {file_path}')
        return

    if ('header_record_client_id' in file_meta and
            headerrecordclientid_map[file_meta['header_record_client_id'].upper()] != file_meta['firm_name'].upper()):
        logger.warning(f'File header_record_client_id {file_meta["header_record_client_id"]} is not mathing with expected Firm name {file_meta["firm_name"]}, skipping {file_path}')
        return

    convert_nfs2_to_json(file_meta=file_meta)



process_single_nfs2(file_path=r'C:\myworkdir\data\NFS2_FSC\FSC_NFS_ACCTBALD_S_230224.DAT')




# %%

@catch_error(logger)
def iterate_over_all_nfs2(source_path:str):
    """
    Main function to iterate over all the files in source_path and add bulk_id
    """
    for root, dirs, files in os.walk(source_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                    extract_dir = tmpdir
                    logger.info(f'Extracting {file_path} to {extract_dir}')
                    shutil.unpack_archive(filename=file_path, extract_dir=extract_dir, format='zip')
                    iterate_over_all_nfs2(source_path=extract_dir)
                continue

            if file_ext.lower() != '.dat':
                logger.warning(f'Not a .DAT file: {file_path}')
                continue

            process_single_nfs2(file_path=file_path)



iterate_over_all_nfs2(source_path=data_settings.source_path)



# %% Close Connections / End Program

mark_execution_end()


# %%


