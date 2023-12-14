# %%

__description__ = """

Load NFS files for Saviynt

"""


# %% Start Logging

import os
from saviynt_modules.logger import catch_error, environment, logger
logger.set_logger(app_name=os.path.basename(__file__))



# %% Parse Arguments

if environment.environment >= environment.qa:
    import argparse

    parser = argparse.ArgumentParser(description=__description__)

    parser.add_argument('--pipeline_key', '--pk', help='pipeline_key value for getting pipeline settings', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipeline_key': 'saviynt_nfs_raa',
        }



# %% Import Libraries

import re, json, shutil, tempfile
from datetime import datetime
from collections import defaultdict
from distutils.dir_util import remove_tree
from collections import OrderedDict

from saviynt_modules.settings import get_csv_rows, normalize_name
from saviynt_modules.migration import recursive_migrate_all_files, get_config, file_meta_exists_in_history



# %% Parameters

args |=  {
    'final_file_ext': '.json',
    'header_record': 'H',
    'data_record': 'D',
    'trailer_record': 'T',
    'final_file_date_format': r'%Y%m%d'
    }



# %% Get Config

config = get_config(args=args)

config.date_threshold = datetime.strptime(config.date_threshold, r'%Y-%m-%d')



# %% Clean up the source path for new files

if os.path.isdir(config.source_path): remove_tree(directory=config.source_path, verbose=0, dry_run=0)
os.makedirs(config.source_path, exist_ok=True)



# %% Parameters

headerrecordclientid_map = config.convert_string_map_to_dict(map_str=config.clientid_map, uppercase_key=True, uppercase_val=True)



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
        'overlap': row['overlap'].strip().lower(),
    }
    return clean_row



# %%

@catch_error(logger)
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
    if len(datetime_str.strip())==8 and 'YYYY' not in format.upper():
        format = format.strip().upper().replace('YY', 'YYYY')

    f = format.strip().upper().replace('YYYY', r'%Y').replace('YY', r'%y').replace('MM', r'%m').replace('DD', r'%d')
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

            file_name_noext = os.path.splitext(os.path.basename(file_path))[0].replace('.', '_')
            file_meta['key_datetime'] = convert_header_datetime(datetime_str=key_datetime, format=key_datetime_format)
            file_meta['json_file_path'] = os.path.join(config.source_path,
                                                       file_meta['table_name']
                                                       + '.' + file_meta['key_datetime'].strftime(config.final_file_date_format)
                                                       + '.' + file_name_noext
                                                       + config.final_file_ext)
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

@catch_error(logger)
def extract_values_from_line(line:str, record_schema:list):
    """
    Extract all values from single line string based on its schema
    """
    if not record_schema:
        raise ValueError(f'record_schema is empty for line {line}')

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
            field_value = str(float(field_value[:x] + '.' + field_value[x:]))

        field_values[field['column_name']] = field_value

    return field_values



# %%

@catch_error(logger)
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

@catch_error(logger)
def process_lines_user_id_administration(fsource, ftarget, file_meta:dict):
    """
    Process all lines for user_id_administration table
    """
    get_record_schema = lambda record_number: all_schema[(file_meta['file_type'], 'record_'+record_number.lower())]

    record_descriptions = {
        '1': 'Portal ID',
        '2': 'Account Linking',
        '3': 'Product',
        '4': 'Federation',
    }

    record_descriptions = {k.strip():normalize_name(v) for k, v in record_descriptions.items()}

    main_record = {}
    columns = {}
    firstln = True
    for line in fsource:
        common_records = {
            'record_type': line[0:1],
            'record_number': line[1:2],
            'portal_user_id': line[2:62].strip(),
            }

        is_data_record_type = common_records['record_type'] == 'D'
        if (not is_data_record_type) and (not main_record):
            continue

        record_number = common_records['record_number']
        if record_number == '1' or not is_data_record_type:
            if main_record:
                if not firstln: ftarget.write(',\n')
                firstln = False
                ftarget.write(json.dumps(main_record))
                columns = columns | main_record.keys()
                if not is_data_record_type: break

            record_schema = get_record_schema(record_number=record_number)
            main_record = new_record(file_meta=file_meta)
            main_record = {**main_record, **extract_values_from_line(line=line, record_schema=record_schema)}
            main_record = {**main_record, **{v:list() for k, v in record_descriptions.items() if k != '1'}}

        if not main_record:
            logger.warning(f'Found line with no D1 record: {line}')
            continue

        if any(common_records[k]!=main_record[k] for k in ['portal_user_id']):
            logger.warning(f'portal_user_id does not match for the same record: {main_record}\nline: {line}')
            continue

        if record_number == '1' or not is_data_record_type:
            pass # placeholder as the condition has already been evaluated above
        elif record_number in ['2', '3', '4']:
            record_schema = get_record_schema(record_number=record_number)
            record = extract_values_from_line(line=line, record_schema=record_schema)
            main_record[record_descriptions[record_number]].append(record)
        else:
            logger.warning(f'Unknown Record Number: {record_number} for line {line}')
            continue

    columns = OrderedDict([(c, config.default_data_type) for c in columns])
    return columns



# %%

process_lines_map = { # Select only what is needed for Saviynt
    'user_id_administration': process_lines_user_id_administration,
    #'user_id_administration_iws': process_lines_user_id_administration,
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
            #ftarget.write('[\n')
            columns = process_lines_map[file_meta['table_name_no_firm']](fsource=fsource, ftarget=ftarget, file_meta=file_meta)
            #ftarget.write('\n]')

    if not columns:
        logger.warning(f'No Data in the file {source_file_path} -> Skipping conversion to JSON and deleting {target_file_path}.')
        os.remove(target_file_path)
        return

    config.columns = columns # to indirectly get json file columns

    logger.info(f'Finished converting file {source_file_path} to JSON file {target_file_path}')

    recursive_migrate_all_files(file_type='json', file_paths=target_file_path, config=config)
    logger.info(f'Deleting file {target_file_path}')
    os.remove(target_file_path)



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

    if config.date_threshold > file_meta['key_datetime']:
        logger.info(f'File datetime {file_meta["key_datetime"]} is older than the datetime threshold {config.date_threshold}, skipping {file_path}')
        return

    for firm_id_field in ['header_record_client_id', 'super_branch']:
        if firm_id_field in file_meta \
            and file_meta[firm_id_field] \
            and headerrecordclientid_map[file_meta[firm_id_field].upper()] != file_meta['firm_name'].upper():
            logger.warning(f'File header_record_client_id {file_meta[firm_id_field]} is not matching with expected Firm name {file_meta["firm_name"]}, skipping {file_path}')
            return

    if file_meta_exists_in_history(config=config, file_name=file_meta['json_file_path']):
        logger.info(f"File already exists, skipping: {file_meta['json_file_path']}")
        return

    convert_nfs2_to_json(file_meta=file_meta)



# %%

@catch_error(logger)
def iterate_over_all_nfs2(remote_path:str):
    """
    Main function to iterate over all the files in source_path and add bulk_id
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

logger.mark_run_end()


# %%


