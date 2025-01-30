description = """

Convert NFS2 fixed-width files to json format

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_NFS_TEST',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\nfs2_schema\nfs2_schema.csv',
        'source_path': r'C:\myworkdir\data\NFS2_Test',
        'db_name': 'ASSETS',
        'schema_name': 'NFS2',
        'clientid_map': 'MAJ:RAA,FXA:SPF,FL2:FSC,00133:SAI,00436:TRI,WDB:WFS,0KS:SAI,TR1:TRI,033:TST',
        'REJECT_FILES':'RMD,OPENLOT,CLOSEDLOT,ORDER,SCHDEVNT,SUITBASE',
        'file_history_start_date': '2016-01-15',
        'pipeline_firm': 'TST',
        'is_full_load': 'FALSE',
        }



# %% Import Libraries

import os, sys, tempfile, shutil, json, re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error,data_settings , logger, mark_execution_end, get_csv_rows, normalize_name, convert_string_map_to_dict, zip_delete_1_file, find_latest_folder,find_latest_file,find_files, max_folder_name
from modules3.migrate_files import file_meta_exists_in_history

from collections import defaultdict
from distutils.dir_util import remove_tree
from datetime import datetime
from typing import List, Dict, Union



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
        'overlap': row['overlap'].strip().lower(),
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
            if row['table_name'] in ['bookkeeping', 'account_balance', 'trade_revenue', 'position', 'order', 'rmd', 'scheduled_events', 'suitability', 'tas_closed', 'tas_open', 'user_id_administration', 'wealthscape_id', 'name_and_address']:
                client_id_iws = HEADER[1:6].strip() # get client id for IWS files (different from NFS headers).
                if len(client_id_iws)>3:
                    table_suffix = '_iws'
            if row['table_name'] in ['name_and_address']:
                H2 = HEADER2[:50].strip().upper() 
                if H2 == 'CH SECURITIESAMERICA':
                    client_id_iws = '00133'
                    table_suffix = '_iws'
                elif H2 == 'CH TRIAD ADVISORS':
                    client_id_iws = '00436'
                    table_suffix = '_iws'

            table_name_no_firm = (row['table_name'] + table_suffix).lower()
            is_full_load = data_settings.is_full_load.upper() == 'TRUE'
            if 'user_id_administration'.lower() in table_name_no_firm.lower():
                is_full_load = True

            file_meta = {
                'database_name': data_settings.domain_name,
                'schema_name': data_settings.schema_name,
                'firm_name': data_settings.pipeline_firm.upper(),
                'table_name_no_firm': table_name_no_firm,
                'table_name': '_'.join([data_settings.pipeline_firm, table_name_no_firm]).lower(),
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

@catch_error(logger)
def extract_values_from_line(line:str, record_schema:list,line_number:str=None):
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
                logger.warning(f'Length of number {field_value} is less than the decimal number {field["decimals"]} for field "{field["column_name"]}"')
            else:
                field_value = str(float(field_value[:x] + '.' + field_value[x:]))

        field_values[field['column_name']] = field_value
        
    if line_number:
        field_values['file_line_number'] = line_number
    
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
def process_lines_1_record(fsource, ftarget, file_meta:dict):
    """
    Process all lines for files that has only 1 record type
    """

    if any(keyword in os.path.basename(fsource.name).upper() for keyword in (data_settings.exclude_file_list.split(','))):        
        return False

    record_schema = all_schema[(file_meta['file_type'], 'record')]

    first = True
       
    line_number = 0 if 'bookkeeping' in file_meta['table_name_no_firm'] else None
    
    for line in fsource:
        if line[0]!='D':
            continue

        line_number = line_number+1 if line_number is not None else None
        
        record = new_record(file_meta=file_meta)
        record = {**record, **extract_values_from_line(line=line, record_schema=record_schema,line_number=line_number)}

        if not first:
            ftarget.write(',\n')

        ftarget.write(json.dumps(record))
        first = False

    return not first



# %%

@catch_error(logger)
def process_lines_name_and_address(fsource, ftarget, file_meta:dict):
    """
    Process all lines for name_and_address file
    """
   ## if 'NAMED' in os.path.basename(fsource.name).upper(): # Use only full files
    ##    return False

    ## Workaround to load delta due to large volume of full file    
    
    if 'NAMEF' in os.path.basename(fsource.name).upper(): # Use only delta files
        return False

    get_record_schema = lambda record_number: all_schema[(file_meta['file_type'], 'record_'+record_number.lower())]

    record_descriptions = {
        '101': ('Customer', None),
        '102': ('FFR list', list),
        '102x': ('FFR', dict),
        '103': ('FFR list', list),
        '104': ('FBSI', dict),
        '107': ('Employer', dict),
        '113': ('FFR list', list),
        '115': ('Legal', dict),
        '2X0': ('Customer', list),
        '2X1': ('Mailing address', list),
        '2X2': ('Legal address', list),
        '2X3': ('Affiliation address', list),
        '3X0': ('Email', list),
        '900': ('ASTK_TCPN', list), # Account Stakeholder / Trusted Contact Person
        '901': ('IPCS', list),
        '998': ('IPID', list),
    }
    record_descriptions = {k.strip():(normalize_name(v[0]),v[1]) for k, v in record_descriptions.items()}

    main_record = {}
    firstln = True
    for line in fsource:
        common_records = {
            'record_type': line[0:1],
            'record_number': line[1:4],
            'firm': line[4:8],
            'branch': line[8:11],
            'account_number': line[11:17],
            }

        is_data_record_type = common_records['record_type'] == 'D'
        if (not is_data_record_type) and (not main_record):
            continue

        record_number = common_records['record_number']
        if record_number == '101' or not is_data_record_type:
            if main_record:
                if not firstln: ftarget.write(',\n')
                firstln = False
                ftarget.write(json.dumps(main_record))
                if not is_data_record_type: break

            record_schema = get_record_schema(record_number=record_number)
            main_record = new_record(file_meta=file_meta)
            main_record = {**main_record, **extract_values_from_line(line=line, record_schema=record_schema)}
            main_record = {**main_record, **{v[0]:v[1]() for k, v in record_descriptions.items() if v[1]}}
            main_record['ffr']['ffr_name_count'] = 0

        if not main_record:
            logger.warning(f'Found line with no 101 record: {line}')
            continue

        if any(common_records[k]!=main_record[k] for k in ['firm', 'branch', 'account_number']):
            if record_number[0]!='9':
                logger.warning(f'Firm, Branch or Account Number does not match for the same record: {main_record}\nline: {line}')
            continue

        if record_number == '101' or not is_data_record_type:
            pass # placeholder as the condition has already been evaluated above
        elif record_number in ['102', '103', '113']:
            record_schema = get_record_schema(record_number=record_number)
            ffr_record = extract_values_from_line(line=line, record_schema=record_schema)

            ffr_prefix = ['x1', 'x2', 'x3']
            ffr_record_prefix = defaultdict(dict)

            for rkey, rval in ffr_record.items():
                if rkey in common_records:
                    continue

                if rkey == 'ffr_name_count':
                    main_record['ffr']['ffr_name_count']  += int(rval if rval else 0)
                    continue

                prefix = rkey[:2].lower()
                if prefix in ffr_prefix:
                    suffix = rkey[2:]
                    is_business = ffr_record[prefix+'ffr_name_type'].upper() == 'B'
                    field_properties = get_field_properties(column_name=rkey, record_schema=record_schema)
                    overlap_value = field_properties['overlap'].upper()
                    if (is_business and overlap_value != 'P') or (not is_business and overlap_value != 'B'):
                        ffr_record_prefix[prefix][suffix] = rval
                else:
                    rkey1 = rkey
                    ffr_str = 'ffr_'.lower()
                    if rkey1[:len(ffr_str)].lower() != ffr_str:
                        rkey1 = ffr_str + rkey1

                    if main_record['ffr'].get(rkey1):
                        raise ValueError(f'Name "{rkey1}" already exists in Main Record {main_record["ffr"]} , check line {line}')
                    main_record['ffr'][rkey1] = rval

            ffr_list = main_record[record_descriptions['102'][0]]
            for _, val in ffr_record_prefix.items():
                if val['ffr_primary_acct_name'] or val['ffr_ssn_number'] or val['ffr_xref']:
                    val2 = {'ffr_'+k:v for k,v in common_records.items()}
                    val2 = {**val2, **val}
                    ffr_list.append(val2)

        elif record_number in ['104', '107', '115']: # dict records
            record_schema = get_record_schema(record_number=record_number)
            record = extract_values_from_line(line=line, record_schema=record_schema)

            rdict = main_record[record_descriptions[record_number][0]]
            for key, val in record.items():
                if rdict.get(key):
                    raise ValueError(f'Name "{key}" already exists in Main Record {rdict} , check line {line}')
                rdict[key] = val

        elif record_number[0] in ['2', '3', '9']: # list records
            if record_number[0] in ['2', '3']:
                record_numberx = record_number[0] + 'X' + record_number[2]
            elif record_number in ['900']:
                record_numberx = record_number
            elif '901' <= record_number <= '997':
                record_numberx = '901'
            elif record_number in ['998', '999']:
                record_numberx = '901' # 998 - Manual override - since there is no specific schema for 998, use 901 schema instead
            else:
                raise ValueError(f'Unknown Record Number: {record_number} for line {line}')

            record_schema = get_record_schema(record_number=record_numberx)
            record = extract_values_from_line(line=line, record_schema=record_schema)

            rlist = main_record[record_descriptions['998'][0]]
            rlist.append(record)

        else:
            logger.warning(f'Unknown Record Number: {record_number} for line {line}')
            continue

    return True if main_record else False



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

    return True if main_record else False



# %%

process_lines_map = {
    'bookkeeping': process_lines_1_record,
    'bookkeeping_iws': process_lines_1_record,
    'account_balance': process_lines_1_record,
    'account_balance_iws': process_lines_1_record,
    'trade_revenue': process_lines_1_record,
    'trade_revenue_iws': process_lines_1_record,
    'name_and_address': process_lines_name_and_address,
    'name_and_address_iws': process_lines_name_and_address,
    'position': process_lines_1_record,
    'position_iws': process_lines_1_record,
    'order': process_lines_1_record,
    'order_iws': process_lines_1_record,
    'rmd': process_lines_1_record,
    'rmd_iws': process_lines_1_record,
    'scheduled_events': process_lines_1_record,
    'scheduled_events_iws': process_lines_1_record,
    'suitability': process_lines_1_record,
    'suitability_iws': process_lines_1_record,
    'tas_closed': process_lines_1_record,
    'tas_closed_iws': process_lines_1_record,
    'tas_open': process_lines_1_record,
    'tas_open_iws': process_lines_1_record,
    'wealthscape_id': process_lines_1_record,
    'wealthscape_id_iws': process_lines_1_record,
    'user_id_administration': process_lines_user_id_administration,
    'user_id_administration_iws': process_lines_user_id_administration,
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

    target_file_path_zip = zip_delete_1_file(file_path=target_file_path)
    logger.info(f'Finished archiving file {target_file_path} to zip file {target_file_path_zip}')



# %%

@catch_error(logger)
def process_single_nfs2(file_path:str):
    """
    Process single nfs2 file
    """
    logger.info(f'Processing {file_path}')
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

    for firm_id_field in ['header_record_client_id', 'super_branch']:
        if firm_id_field in file_meta \
            and file_meta[firm_id_field] \
            and headerrecordclientid_map[file_meta[firm_id_field].upper()] != file_meta['firm_name'].upper():
            logger.warning(f'File header_record_client_id {file_meta[firm_id_field]} is not mathing with expected Firm name {file_meta["firm_name"]}, skipping {file_path}')
            return

    convert_nfs2_to_json(file_meta=file_meta)



# %%

@catch_error(logger)
def iterate_over_all_nfs2(source_path:str):
    """
    Main function to iterate over all the files in source_path and add bulk_id
    """
    if not source_path:
        logger.warning('Empty Source Path - skipping')
        return

    logger.info(f'Checking path: {source_path}')
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

if 'LFA_DAILY' in data_settings.pipelinekey.upper() or 'LFS_DAILY' in data_settings.pipelinekey.upper():
    if data_settings.file_pattern:
        file_name_patterns = data_settings.file_pattern.split(',')
        print('file_name_patterns', str(file_name_patterns))
        for file_name_pattern in file_name_patterns:
            matching_files = find_files(source_path=data_settings.source_path, pattern=file_name_pattern)
            for file in matching_files:
                process_single_nfs2(file)
                 


elif 'LFA_FULL' in data_settings.pipelinekey.upper() or 'LFS_FULL' in data_settings.pipelinekey.upper():
    if data_settings.find_latest_file_name_pattern:
        file_name_patterns = data_settings.find_latest_file_name_pattern.split(',')
        print('file_name_patterns', str(file_name_patterns))
        for file_name_pattern in file_name_patterns:
            latest_file = find_latest_file(source_path=data_settings.source_path, pattern=file_name_pattern)
            if latest_file:
                process_single_nfs2(latest_file)
                process_single_nfs2(latest_file)
else:
    iterate_over_all_nfs2(source_path=data_settings.source_path)




if hasattr(data_settings, 'source_path2'):
    iterate_over_all_nfs2(source_path=data_settings.source_path2)

if 'HISTORY' in data_settings.pipelinekey.upper():
    pass

elif data_settings.pipeline_firm.lower() == 'sai':
    base_path = r'/opt/EDIP/remote/fasoma05bprd/DownloadData/_SAI/MIPS'
    folder_levels = [r'%Y', r'%b', 'NFSBOOK'] # for bookkeeping data
    source_path = find_latest_folder(remote_path=base_path, folder_levels=folder_levels)
    logger.info(f'source_path for bookkeeping data: {source_path}')
    iterate_over_all_nfs2(source_path=source_path)

    base_path = r'/opt/EDIP/remote/fasoma05bprd/DownloadData/_SAI'
    folder_levels = [r'%Y', r'%b', r'%Y%m%d', 'FIDELITYIWS', max_folder_name, 'RAWDATA'] # for IWS data
    source_path = find_latest_folder(remote_path=base_path, folder_levels=folder_levels)
    logger.info(f'source_path for IWS data: {source_path}')
    iterate_over_all_nfs2(source_path=source_path)

elif data_settings.pipeline_firm.lower() == 'tri':
    base_path = r'/opt/EDIP/remote/fasoma05bprd/DownloadData/_TRI'
    folder_levels = [r'%Y', r'%b', r'%Y%m%d', 'NFS', max_folder_name, 'RAWDATA'] # for NFS data
    source_path = find_latest_folder(remote_path=base_path, folder_levels=folder_levels)
    logger.info(f'source_path for NFS data: {source_path}')
    iterate_over_all_nfs2(source_path=source_path)

    base_path = r'/opt/EDIP/remote/fasoma05bprd/DownloadData/_TRI'
    folder_levels = [r'%Y', r'%b', r'%Y%m%d', 'FIDELITYIWS', max_folder_name, 'RAWDATA'] # for IWS data
    source_path = find_latest_folder(remote_path=base_path, folder_levels=folder_levels)
    logger.info(f'source_path for IWS data: {source_path}')
    iterate_over_all_nfs2(source_path=source_path)



# %% Close Connections / End Program

mark_execution_end()


# %%


