# %% Description

__description__ = """

Load NFS files for Saviynt

"""


# %% Import Libraries

import os, re, shutil, tempfile, csv
from datetime import datetime
from collections import defaultdict
from distutils.dir_util import remove_tree
from collections import OrderedDict

from saviynt_modules.settings import init_app, get_csv_rows, normalize_name
from saviynt_modules.logger import logger, catch_error
from saviynt_modules.migration import recursive_migrate_all_files, file_meta_exists_in_history
from saviynt_modules.common import common_delimiter, picture_to_decimals



# %% Parameters

test_pipeline_key = 'saviynt_nfs_raa'

args = {
    'final_file_ext': '.psv',
    'header_record': 'H',
    'data_record': 'D',
    'trailer_record': 'T',
    }



# %% Get Config

config = init_app(
    __file__ = __file__,
    __description__ = __description__,
    args = args,
    test_pipeline_key = test_pipeline_key,
)



# %% Clean up the source path for new files

if os.path.isdir(config.source_path): remove_tree(directory=config.source_path, verbose=0, dry_run=0)
os.makedirs(config.source_path, exist_ok=True)



# %% Parameters

headerrecordclientid_map = config.convert_string_map_to_dict(map_str=config.clientid_map, uppercase_key=True, uppercase_val=True)



# %%

@catch_error()
def clean_row(row):
    """
    Use cleaned up / normalized version of the row data
    """
    if not row:
        return {}

    pic = row['source_field_pic'].upper().replace('PIC','')
    decimals = picture_to_decimals(pic=pic)

    cleaned_row = {
        'file_type': normalize_name(row['source_file_description']),
        'column_name': normalize_name(row['business_name']),
        'record_number': normalize_name(row['record_number']),
        'table_name': normalize_name(row['table_name']),
        'file_title': row['file_title'].strip().lower(),
        'pos_start': int(row['source_field_start_pos'].strip()) - 1,
        'pos_end': int(row['source_field_end_pos'].strip()),
        'format': row['format'].strip(),
        'decimals': decimals,
        'overlap': row['overlap'].strip().lower(),
    }
    return cleaned_row



# %%

@catch_error()
def get_nfs_schema():
    """
    Get Header file_title position information from schema
    """
    schema_file_path = config.schema_path
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


file_titles = {x: file_titles[x] for x in ['user_id_administration_transmission']} # limit to Saviyt files



# %%

@catch_error()
def convert_header_datetime(datetime_str:str, format:str):
    """
    Convert datetime string based on specific format to generic datetime object
    MMDDYY
    MMDDYYYY
    MM/DD/YYYY
    YYMMDD
    YYYYMMDD
    """
    if len(datetime_str.strip())==8 and 'YYYY' not in format.upper():
        format = format.strip().upper().replace('YY', 'YYYY')

    f = format.strip().upper().replace('YYYY', r'%Y').replace('YY', r'%y').replace('MM', r'%m').replace('DD', r'%d')
    return datetime.strptime(datetime_str, f)



# %%

@catch_error()
def extract_nfs2_file_meta(file_path:str, zip_file_path:str=None):
    """
    Get file header info, and filter unwanted files.
    """
    file_name = os.path.basename(file_path)

    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()
        HEADER2 = f.readline()

    file_meta = None
    for file_type, row in file_titles.items():
        file_title = HEADER[row['pos_start']:row['pos_end']].strip().lower()
        if file_title == 'NAME AND ADDRESS'.lower():
            file_title = 'NAME/ADDR HISTORY'.lower() # Fix for SAI and TRI IWS files
        elif file_title == 'POSITION DELTA E'.lower():
            file_title = 'POSITION EXTRACT'.lower()
        elif file_title == 'STREETSCAPE ID'.lower():
            file_title = 'WEALTHSCAPE ID'.lower()

        if file_title.lower() == row['file_title'].lower():
            header_schema = all_schema[(file_type, 'header')]

            table_suffix = ''
            client_id_iws = ''
            if row['table_name'] in ['bookkeeping', 'account_balance', 'trade_revenue', 'position', 'order', 'rmd', 'scheduled_events', 'suitability', 'tas_closed', 'tas_open', 'user_id_administration', 'wealthscape_id']:
                client_id_iws = HEADER[1:6].strip() # get client id for IWS files (different from NFS headers).
                if len(client_id_iws)>3:
                    table_suffix = '_iws'
            elif row['table_name'] in ['name_and_address']:
                H2 = HEADER2[:50].strip().upper() 
                if H2 == 'CH SECURITIESAMERICA':
                    client_id_iws = '00133'
                    table_suffix = '_iws'
                elif H2 == 'CH TRIAD ADVISORS':
                    client_id_iws = '00436'
                    table_suffix = '_iws'

            table_name_no_firm = (row['table_name'] + table_suffix).lower()
            is_full_load = config.is_full_load.upper() == 'TRUE'
            if 'user_id_administration'.lower() in table_name_no_firm.lower():
                is_full_load = True

            file_meta = {
                'firm_name': config.firm_name.upper(),
                'table_name_no_firm': table_name_no_firm,
                'table_name': table_name_no_firm, # '_'.join([data_settings.pipeline_firm, table_name_no_firm]).lower(),
                'file_type': row['file_type'],
                'is_full_load': is_full_load,
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

            if client_id_iws: # overwirte client_id for select files
                file_meta['header_record_client_id'] = client_id_iws

            for x in ['transmission_creation_date', 'run_date', 'header_date']:
                if file_meta.get(x):
                    key_datetime = file_meta[x]
                    key_datetime_format = header_info_format[x]
                    break

            file_meta['file_name_noext'] = os.path.splitext(os.path.basename(file_path))[0].replace('.', '_')

            file_meta['key_datetime'] = convert_header_datetime(datetime_str=key_datetime, format=key_datetime_format)
            file_meta['out_file_path'] = os.path.join(config.source_path,
                                                       file_meta['table_name']
                                                       + '.' + file_meta['key_datetime'].strftime(config.file_date_format)
                                                       + '.' + file_meta['file_name_noext']
                                                       + config.final_file_ext)
            break

    return file_meta



# %%

@catch_error()
def new_record(file_meta:dict):
    """
    Create new record with basic info
    """
    record = OrderedDict([
        ('header_firm_name', file_meta['firm_name']),
        ])

    if 'header_record_client_id' in file_meta:
        record['header_client_id'] = file_meta['header_record_client_id']

    return record



# %%

@catch_error()
def extract_values_from_line(line:str, record_schema:list):
    """
    Extract all values from single line string based on its schema
    """
    if not record_schema:
        raise ValueError(f'record_schema is empty for line {line}')

    field_values = OrderedDict()
    for field in record_schema:
        field_value = line[field['pos_start']:field['pos_end']]
        field_value = re.sub(' +', ' ', field_value.strip())

        if field['decimals']>0 and field_value:
            if not field_value.isdigit():
                raise ValueError(f'Schema Scale mismatch for field "{field["column_name"]}" field value "{field_value}". Field Value should be all digits!')
            x = len(field_value) - field['decimals']
            if x<0:
                raise ValueError(f'Length of number {field_value} is less than the decimal number {field["decimals"]} for field "{field["column_name"]}"')
            field_value = str(float(field_value[:x] + '.' + field_value[x:]))

        field_values[field['column_name']] = field_value

    return field_values



# %%

@catch_error()
def get_field_properties(column_name:str, record_schema:list):
    """
    Extract all the properties of field for given column_name
    """
    field_properties = {}
    for field in record_schema:
        if field['column_name'].lower() == column_name.lower():
            field_properties = field
            break
    return field_properties



# %%

@catch_error()
def process_lines_user_id_administration(file_meta:dict):
    """
    Process all lines for user_id_administration table
    """
    logger.info(f"Processing user_id_administration file {file_meta['file_path']}")
    logger.debug(file_meta)

    get_record_schema = lambda record_number: all_schema[(file_meta['file_type'], 'record_'+record_number.lower())]

    record_descriptions = {
        '1': 'Portal ID',
        '2': 'Account Linking',
        '3': 'Product',
        '4': 'Federation',
    }

    record_descriptions = {k.strip():normalize_name(v) for k, v in record_descriptions.items()}

    record_files = dict()
    for record_number, record_desription in record_descriptions.items():
        file_path = os.path.join(config.source_path,
                                    file_meta['table_name']
                                    + (('_'+record_desription) if record_number != '1' else '')
                                    + '.' + file_meta['key_datetime'].strftime(config.file_date_format)
                                    + '.' + file_meta['file_name_noext']
                                    + config.final_file_ext)

        logger.debug(f'file_path = {file_path}')

        record_files[record_number] = {
            'file': open(file=file_path, mode='wt', encoding='utf-8'),
            'file_paths': file_path,
            'writer': None,
            'first': True,
            }

    with open(file_meta['file_path'], mode='rt', encoding='ISO-8859-1', errors='ignore') as fsource:
        for line in fsource:
            record_type = line[0:1]
            record_number = line[1:2]
            if record_type != 'D': continue

            record_schema = get_record_schema(record_number=record_number)
            record = new_record(file_meta=file_meta) | extract_values_from_line(line=line, record_schema=record_schema)

            if record_files[record_number]['first']:
                record_files[record_number]['first'] = False
                record_files[record_number]['writer'] = csv.DictWriter(record_files[record_number]['file'], delimiter=common_delimiter, quotechar=None, quoting=csv.QUOTE_NONE, skipinitialspace=True, fieldnames=record.keys())
                record_files[record_number]['writer'].writeheader()

            record_files[record_number]['writer'].writerow(record)

    for record_number, record_file in record_files.items():
        record_file['file'].close()

    out_files = [record_file['file_paths'] for record_number, record_file in record_files.items()]
    logger.info(f'Files created: {out_files}')

    return out_files



# %%

process_lines_map = { # Select only what is needed for Saviynt
    'user_id_administration': process_lines_user_id_administration,
    #'user_id_administration_iws': process_lines_user_id_administration,
}



# %%

@catch_error()
def convert_nfs2_to_psv(file_meta:dict):
    """
    Convert given NFS2 DAT file to Json format.
    """
    target_file_paths =  process_lines_map[file_meta['table_name_no_firm']](file_meta=file_meta)

    if not target_file_paths:
        logger.warning(f"Could not process {file_meta['file_path']} skipping.")
        return

    logger.info(f"Finised converting {file_meta['file_path']} to {target_file_paths}")

    for target_file_path in target_file_paths:
        recursive_migrate_all_files(file_type='csv', file_paths=target_file_path, config=config)
        logger.info(f'Deleting file {target_file_path}')
        os.remove(path=target_file_path)



# %%

@catch_error()
def process_single_nfs2(file_path:str):
    """
    Process single nfs2 file
    """
    file_meta = extract_nfs2_file_meta(file_path=file_path)
    if not file_meta:
        logger.warning(f'Unable to get header info for file: {file_path} -> skipping')
        return

    logger.debug(f'file_meta = {file_meta}')

    if config.date_threshold > file_meta['key_datetime']:
        logger.info(f'File datetime {file_meta["key_datetime"]} is older than the datetime threshold {config.date_threshold}, skipping {file_path}')
        return

    for firm_id_field in ['header_record_client_id', 'super_branch']:
        if firm_id_field in file_meta \
            and file_meta[firm_id_field] \
            and headerrecordclientid_map[file_meta[firm_id_field].upper()] != file_meta['firm_name'].upper():
            logger.warning(f'File header_record_client_id {file_meta[firm_id_field]} is not matching with expected Firm name {file_meta["firm_name"]}, skipping {file_path}')
            return

    if file_meta_exists_in_history(config=config, file_path=file_meta['out_file_path']):
        logger.info(f"File already exists, skipping: {file_meta['out_file_path']}")
        return

    convert_nfs2_to_psv(file_meta=file_meta)



# %%

@catch_error()
def iterate_over_all_nfs2(remote_path:str):
    """
    Main function to iterate over all the files
    """
    if not remote_path:
        logger.warning('Empty Remote Path - skipping')
        return

    for root, dirs, files in os.walk(remote_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=config.temporary_folder_path) as tmpdir:
                    extract_dir = tmpdir
                    logger.info(f'Extracting {file_path} to {extract_dir}')
                    shutil.unpack_archive(filename=file_path, extract_dir=extract_dir, format='zip')
                    iterate_over_all_nfs2(remote_path=extract_dir)
                continue

            if file_ext.lower() != '.dat':
                logger.warning(f'Not a .DAT file: {file_path}')
                continue

            process_single_nfs2(file_path=file_path)



iterate_over_all_nfs2(remote_path=config.remote_path)



# %% Close Connections / End Program

logger.mark_ending()


# %%


