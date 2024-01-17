# %% Description

__description__ = """

Load Pershing files for Saviynt

"""


# %% Import Libraries

import os, re, shutil
from datetime import datetime
from zipfile import ZipFile
from collections import defaultdict
from distutils.dir_util import remove_tree

from saviynt_modules.settings import init_app, get_csv_rows, normalize_name
from saviynt_modules.logger import logger, catch_error
from saviynt_modules.common import remove_last_line_from_file
from saviynt_modules.migration import recursive_migrate_all_files



# %% Parameters

test_pipeline_key = 'saviynt_pershing_raa'

master_schema_header_columns = {
    'date_of_data': 'datetime NULL',
    'remote_id': 'varchar(50) NULL',
    'refreshed_updated': 'varchar(10) NULL',
    }

args = {
    'master_schema_file': 'security_profiles',
    'master_schema_header_columns': master_schema_header_columns,
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



# %% get and pre-process schema

@catch_error(logger)
def get_pershing_schema(schema_file_name:str, table_name:str=''):
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
        field_name = normalize_name(row['field_name'])
        position = row['position'].strip()
        record_name = row['record_name'].upper().strip()
        conditional_changes = row['conditional_changes'].upper().strip()

        if schema_file_name.lower().startswith('security_profiles') and record_name.upper() not in [schema_header_str, schema_trailer_str, table_name[-1].upper()]:
            continue

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
    return header_schema



header_schema = get_header_schema()



# %% Get Header Info from Pershing file

@catch_error(logger)
def get_header_info(file_path:str, is_bulk_formatted:bool=True):
    """
    Get Header Info from Pershing file
    """
    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    if is_bulk_formatted:
        prefix_length = 0
        if HEADER[:len(bulk_id_header)] != bulk_id_header: 
            logger.warning(f'Not a Bulk Formatted file: {file_path}')
            return
    else:
        prefix_length = total_prefix_length

    header_info = dict()
    for field_name, pos in header_schema.items():
        header_info[field_name] = re.sub(' +', ' ', HEADER[pos['position_start']-1-prefix_length: pos['position_end']-prefix_length].strip())

    header_info['date_of_data'] = datetime.strptime(header_info['date_of_data'], r'%m/%d/%Y')
    return header_info



# %%







