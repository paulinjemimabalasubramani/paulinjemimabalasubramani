"""
Common Library for extracting data from Pershing files header.

"""

# %% Import Libraries

import os, hashlib, re

from datetime import datetime

from .common_functions import catch_error, data_settings, logger, column_regex, get_csv_rows



# %% Parameters

bulk_file_ext = '.bulk'

hash_field_name = 'bulk_id'
hash_func = hashlib.sha256
total_hash_length = len(hash_func().hexdigest())
total_prefix_length = total_hash_length + 1

HEADER_str = 'HEADER'
TRAILER_str = 'TRAILER'

schema_header_str = 'HEADER'
schema_trailer_str = 'TRAILER'

bulk_id_header = HEADER_str.ljust(total_hash_length)
bulk_id_trailer = TRAILER_str.ljust(total_hash_length)

master_schema_file = 'customer_acct_info' # to take header info
master_schema_form_name = 'customer_acct_info' # to take header info
master_schema_header_columns = {
    'date_of_data': 'datetime NULL',
    'remote_id': 'varchar(50) NULL',
    'refreshed_updated': 'varchar(10) NULL',
    }



# %% get and pre-process schema

@catch_error(logger)
def get_pershing_schema(schema_file_name:str):
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    if not schema_file_name.endswith('.csv'): schema_file_name = schema_file_name + '.csv'
    schema_file_path = os.path.join(data_settings.schema_file_path, schema_file_name)
    if not os.path.isfile(schema_file_path):
        logger.warning(f'Schema file is not found: {schema_file_path}')
        return

    schema = []
    record_names = []
    for row in get_csv_rows(csv_file_path=schema_file_path):
        field_name = re.sub(column_regex, '_', row['field_name'].lower().strip())
        position = row['position'].strip()
        record_name = row['record_name'].upper().strip()
        conditional_changes = row['conditional_changes'].upper().strip()

        if not field_name or (field_name in ['', 'not_used', 'filler', '_', '__', 'n_a', 'na', 'none', 'null', 'value']) \
            or ('-' not in position) or not record_name: continue

        scale = str(row['scale']).strip()
        scale = int(scale) if scale.isdigit() else -1

        if record_name not in record_names:
            record_names.append(record_name)
            schema.append({
                'field_name': hash_field_name.lower(),
                'position_start': 1,
                'position_end': total_hash_length,
                'length': total_hash_length,
                'scale': scale,
                'record_name': record_name,
                'conditional_changes': conditional_changes,
                })

        pos = position.split('-')
        position_start = int(pos[0]) + total_prefix_length
        position_end = int(pos[1]) + total_prefix_length
        if position_start > position_end:
            raise ValueError(f'position_start {pos[0]} > position_end {pos[1]} for field_name {field_name} in record_name {record_name}')

        schema.append({
            'field_name': field_name,
            'position_start': position_start,
            'position_end': position_end,
            'length': position_end - position_start + 1,
            'scale': scale,
            'record_name': record_name,
            'conditional_changes': conditional_changes,
            })

    return schema



# %% Get Header Schema from Master Pershing Schema

@catch_error(logger)
def get_header_schema():
    """
    Get Header Schema from Master Pershing Schema
    """
    position_fields = ['position_start', 'position_end', 'length']

    schema = get_pershing_schema(schema_file_name=master_schema_file)
    if not schema: raise Exception(f'Main Schema file "{master_schema_file}" is not found!')

    header_schema = [c for c in schema if c['record_name'] == schema_header_str]
    header_schema = {r['field_name']: {field_name: r[field_name] for field_name in position_fields} for r in header_schema}
    header_schema['form_name'] = header_schema.pop(master_schema_form_name)
    return header_schema



header_schema = get_header_schema()



# %% Get Header Info from Pershing file

@catch_error(logger)
def get_header_info(file_path:str):
    """
    Get Header Info from Pershing file
    """
    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    if HEADER[:len(bulk_id_header)] != bulk_id_header: 
        logger.warning(f'Not a Bulk Formatted file: {file_path}')
        return

    header_info = dict()
    for field_name, pos in header_schema.items():
        header_info[field_name] = re.sub(' +', ' ', HEADER[pos['position_start']-1: pos['position_end']].strip())

    header_info['date_of_data'] = datetime.strptime(header_info['date_of_data'], r'%m/%d/%Y')
    return header_info



# %%


