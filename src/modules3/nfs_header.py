"""
Common Library for extracting data from NFS files header.

"""

# %% Import Libraries

import os, re

from collections import defaultdict
from datetime import datetime

from .common_functions import catch_error, data_settings, logger, column_regex



# %% Parameters

table_name_map = {
    'name_addr_history': 'name_and_address',
    'position_extract': 'position',
    'bookkeeping': 'activity',
    'security_master': 'security_master',
}

json_file_ext = '.json'
json_file_date_format = r'%Y%m%d'



# %% Get Client ID Map

@catch_error(logger)
def get_headerrecordclientid_map():
    """
    Get Client ID Map
    """
    headerrecordclientid_map = dict()

    cids = data_settings.clientid_map.split(',')

    for cid in cids:
        cid_split = cid.split(':')
        headerrecordclientid = cid_split[0].strip().upper()
        firm = cid_split[1].strip().upper()
        headerrecordclientid_map = {**headerrecordclientid_map, headerrecordclientid:firm}

    return headerrecordclientid_map



headerrecordclientid_map = get_headerrecordclientid_map()



# %% Get Header Info

@catch_error(logger)
def get_header_info(file_path:str):
    """
    Get Header Info
    """
    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    master_header_schema = defaultdict(dict)

    if HEADER[21:37].upper() == 'FIDELITY SYSTEMS':
        if HEADER[1:3] == '00':
            master_header_schema['headerrecordclientid'] = {'position_start': 3, 'position_end': 6}
        else:
            master_header_schema['headerrecordclientid'] = {'position_start': 1, 'position_end': 4}
        master_header_schema['filetitle'] = {'position_start': 41, 'position_end': 58}
        master_header_schema['transmissioncreationdate'] = {'position_start': 62, 'position_end': 68}
    elif HEADER[12:28].upper() == 'FIDELITY SYSTEMS':
        master_header_schema['headerrecordclientid'] = {'position_start': 3, 'position_end': 6}
        master_header_schema['filetitle'] = {'position_start': 32, 'position_end': 47}
        master_header_schema['transmissioncreationdate'] = {'position_start': 6, 'position_end': 12}
    elif HEADER[36:48].upper() == 'NFSC SYSTEMS':
        master_header_schema['headerrecordclientid'] = {'position_start': 58, 'position_end': 61}
        master_header_schema['filetitle'] = {'position_start': 11, 'position_end': 22}
        master_header_schema['transmissioncreationdate'] = {'position_start': 71, 'position_end': 77}
    else:
        logger.warning(f'File: "{file_path}" has unknown HEADER: {HEADER}')
        return

    header_info = dict()
    for field_name, pos in master_header_schema.items():
        header_info[field_name] = re.sub(' +', ' ', HEADER[pos['position_start']: pos['position_end']].strip())

    table_name = re.sub(column_regex, '_', header_info['filetitle'].lower())
    if table_name not in table_name_map:
        logger.warning(f'Unknown Table Name "{table_name}" in file {file_path}')
        return

    firm_name = headerrecordclientid_map[header_info['headerrecordclientid'].upper()]

    header_info['table_name_no_firm'] = table_name_map[table_name].lower()
    header_info['table_name'] = '_'.join([firm_name, header_info['table_name_no_firm']]).lower()
    header_info['firm_name'] = firm_name

    try:
        header_info['key_datetime'] = datetime.strptime(header_info['transmissioncreationdate'], r'%m%d%y')
    except Exception as e:
        logger.warning(f"Cannot parse transmissioncreationdate: {header_info['transmissioncreationdate']}. {str(e)}")
        return

    file_name_noext = os.path.splitext(os.path.basename(file_path))[0].replace('.', '_')
    header_info['target_file_name'] = file_name_noext + '.' + header_info['table_name'] + '_' + header_info['key_datetime'].strftime(json_file_date_format) + json_file_ext

    logger.info(header_info)
    return header_info



# %%


