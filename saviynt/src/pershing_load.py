# %% Description

__description__ = """

Load Pershing files for Saviynt

"""


# %% Import Libraries

import os, re, shutil, csv, yaml, tempfile
from datetime import datetime
from collections import defaultdict, OrderedDict
from distutils.dir_util import remove_tree
from dataclasses import dataclass
from typing import Dict, List

from saviynt_modules.settings import init_app, get_csv_rows
from saviynt_modules.logger import logger, catch_error, environment
from saviynt_modules.common import picture_to_decimals, common_delimiter, normalize_name
from saviynt_modules.migration import recursive_migrate_all_files, file_meta_exists_in_history



# %% Types

@dataclass
class SchemaHeaderRow:
    data_type:str
    pos_start:int
    pos_end:int


class HeaderSchema:
    header_name = SchemaHeaderRow('varchar(20) NULL', 19, 36)
    date_of_data = SchemaHeaderRow('date NULL', 47, 56)
    remote_id = SchemaHeaderRow('varchar(4) NULL', 68, 71)
    run_date = SchemaHeaderRow('date NULL', 86, 95)
    run_time = SchemaHeaderRow('time NULL', 97, 104)
    refreshed_updated = SchemaHeaderRow('varchar(10) NULL', 119, 127)

    @classmethod
    def to_dict(cls):
        return {c:getattr(cls, c) for c in cls.__dict__ if type(getattr(cls, c))==SchemaHeaderRow}


@dataclass
class OutFile:
    file_path:str = None
    file_obj:object = None
    csv_obj:object = None


@dataclass
class HeaderMap:
    names:Dict
    transactions:Dict
    record_names:Dict



# %% Parameters

test_pipeline_key = 'saviynt_pershing_raa'

args = {
    'HEADER': 'HEADER',
    'TRAILER': 'TRAILER',
    'header_map_file': 'header_map.yaml',
    'header_schema': HeaderSchema,
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



# %%

@catch_error()
def prepare_file_name_like_list():
    """
    Prepae list of file names that will be checked against to filter files.
    """
    file_name_like_list = config.file_name_like_list.strip().upper().split(',')
    file_name_like_list = [f'{x.strip()}' for x in file_name_like_list]
    config.file_name_like_list = file_name_like_list


prepare_file_name_like_list()



# %%

@catch_error()
def get_header_map():
    with open(file=os.path.join(config.schema_path, config.header_map_file), mode='rt', encoding='utf-8', errors='ignore') as f:
        contents = yaml.load(f, Loader=yaml.SafeLoader)

    header_map = contents['headers']

    header_map = HeaderMap(
        names = {h['name']:h['short_name'] for h in header_map},
        transactions = {h['short_name']:h['transaction_code'] for h in header_map},
        record_names = {h['short_name']:h['record_name'] for h in header_map},
    )

    return header_map


config.header_map = get_header_map()



# %% get and pre-process schema

@catch_error()
def get_pershing_schema(schema_file_name:str):
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    schema_file_name = schema_file_name.upper()
    if not schema_file_name.lower().endswith('.csv'): schema_file_name = schema_file_name + '.csv'
    schema_file_path = os.path.join(config.schema_path, schema_file_name)
    if not os.path.isfile(schema_file_path):
        logger.warning(f'Schema file is not found: {schema_file_path}')
        return

    schema = defaultdict(list)
    for row in get_csv_rows(csv_file_path=schema_file_path):
        field_name = normalize_name(row['field_name'])
        position = row['position'].strip()
        record_name = row['record_name'].upper().strip()
        conditional_changes = row['conditional_changes'].upper().strip()
        decimals = picture_to_decimals(pic=row['picture'])

        if not field_name or (field_name in ['', 'not_used', 'filler', '_', '__', 'n_a', 'na', 'none', 'null', 'value']) \
            or ('-' not in position) or not record_name: continue

        pos = position.split('-')
        position_start = int(pos[0].strip()) - 1
        position_end = int(pos[1].strip())
        if position_start > position_end:
            raise ValueError(f'position_start {position_start} > position_end {position_end} for field_name {field_name} in record_name {record_name}')

        schema[record_name].append({
            'field_name': field_name,
            'position_start': position_start,
            'position_end': position_end,
            'decimals': decimals,
            'conditional_changes': conditional_changes,
            })

    return schema



# %% Get Header Info from Pershing file

@catch_error()
def get_header_info(file_path:str):
    """
    Get Header Info from Pershing file
    """
    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()
    if not HEADER: return None

    try:
        header_info = OrderedDict()
        for field_name, field in config.header_schema.to_dict().items():
            header_info[field_name] = re.sub(' +', ' ', HEADER[field.pos_start-1: field.pos_end].strip())

        header_info['date_of_data'] = datetime.strptime(header_info['date_of_data'], r'%m/%d/%Y')

    except Exception as e:
        logger.warning(f'Error in reading header of file {file_path} Exception: {str(e)}')
        return None

    return header_info



# %%

@catch_error()
def get_pershing_file_meta(file_path:str):
    """
    Get metadata about pershing file, decide if file is real pershing file.
    """
    file_meta = get_header_info(file_path=file_path)
    if not file_meta: return None

    header_name = file_meta['header_name']
    if header_name not in config.header_map.names:
        logger.warning(f"{header_name} file {file_path} is not found in config.header_map.names, skipping")
        return None

    short_name = config.header_map.names[header_name]
    transaction_code = config.header_map.transactions[short_name]

    out_file_name = f"{short_name}_{config.header_map.record_names[short_name]}_{file_meta['date_of_data'].strftime(config.file_date_format)}.csv".lower()
    out_file_path = os.path.join(config.source_path, out_file_name)

    file_meta |= {
        'file_path': file_path,
        'short_name': short_name,
        'transaction_code': transaction_code,
        'out_file_path': out_file_path,
    }

    logger.debug(file_meta)
    return file_meta



# %%

@catch_error()
def new_record(file_meta:dict):
    """
    Create new record with basic info
    """
    record = OrderedDict([
        ('header_name', file_meta['header_name']),
        ('header_firm_name', config.firm_name.upper()),
        ('header_remote_id', file_meta['remote_id']),
        ('header_refreshed_updated', file_meta['refreshed_updated']),
        ('header_run_date', file_meta['run_date']),
        ('header_run_time', file_meta['run_time']),
        ])

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
        field_value = line[field['position_start']:field['position_end']]
        field_value = re.sub(' +', ' ', field_value.strip())

        if field['decimals']>0 and field_value:
            if not field_value.isdigit():
                raise ValueError(f'Schema Scale mismatch for field "{field["field_name"]}" field value "{field_value}". Field Value should be all digits!')
            x = len(field_value) - field['decimals']
            if x<0:
                raise ValueError(f'Length of number {field_value} is less than the decimal number {field["decimals"]} for field "{field["field_name"]}"')
            field_value = str(float(field_value[:x] + '.' + field_value[x:]))

        field_values[field['field_name']] = field_value

    return field_values



# %%

@catch_error()
def convert_pershing_to_psv(file_meta:Dict):
    """
    Convert pershing file to PSV. Should work with majority of pershing files
    Currently conditional_changes is not configured
    """
    logger.info(f"Processing {file_meta['short_name']} file {file_meta['file_path']}")
    schema = get_pershing_schema(schema_file_name=file_meta['short_name'])

    with open(file=file_meta['file_path'], mode='rt', encoding='utf-8', errors='replace') as fsource:
        out_files = OrderedDict()

        for line in fsource:
            if line[:3] in ['BOF', 'EOF']: continue
            if line[:2] != file_meta['transaction_code']: 
                logger.warning(f"transaction_code mismatch, expected {file_meta['transaction_code']}, got {line[:2]}, not a valid {file_meta['short_name']} file for line: {line}")
                # return

            record_name = line[2]
            record = new_record(file_meta=file_meta)
            record_schema = schema[record_name]

            if record_name not in out_files:
                out_file_name = f"{file_meta['short_name']}_{record_name}_{file_meta['date_of_data'].strftime(config.file_date_format)}.csv".lower()
                out_file_path = os.path.join(config.source_path, out_file_name)
                file_obj = open(file=out_file_path, mode='wt', encoding='utf-8', newline='')

                fieldnames = list(record.keys()) + [x['field_name'] for x in record_schema]
                csv_obj = csv.DictWriter(f=file_obj, delimiter=common_delimiter, quotechar=None, quoting=csv.QUOTE_NONE, skipinitialspace=True, fieldnames=fieldnames)

                out_files[record_name] = OutFile(file_path=out_file_path, file_obj=file_obj, csv_obj=csv_obj)
                out_files[record_name].csv_obj.writeheader()

            record |= extract_values_from_line(line=line, record_schema=record_schema)
            out_files[record_name].csv_obj.writerow(record)

    out_file_paths = []
    for record_name, out_file in out_files.items():
        out_file.file_obj.close()
        out_file_paths.append(out_file.file_path)

    logger.debug(out_file_paths)
    logger.info(f"Finised converting record files for {file_meta['file_path']} to {config.source_path}")
    return out_file_paths



# %%

@catch_error()
def process_single_pershing(file_path:str):
    """
    Process single Pershing file
    """
    file_meta = get_pershing_file_meta(file_path=file_path)
    if not file_meta:
        logger.warning(f'Unable to get file_meta for file: {file_path} -> skipping')
        return

    if config.date_threshold > file_meta['date_of_data']:
        logger.info(f'File datetime {file_meta["date_of_data"]} is older than the datetime threshold {config.date_threshold}, skipping {file_path}')
        return

    if file_meta_exists_in_history(config=config, file_path=file_meta['out_file_path']):
        logger.info(f"File already exists, skipping: {file_meta['out_file_path']}")
        return

    out_file_paths = convert_pershing_to_psv(file_meta=file_meta)

    if not out_file_paths:
        logger.warning(f"Could not process {file_meta['file_path']} skipping.")
        return

    logger.info(f"Finished converting {file_meta['file_path']} to {out_file_paths}")

    for out_file_path in out_file_paths:
        recursive_migrate_all_files(file_type='csv', file_paths=out_file_path, config=config)
        logger.info(f'Deleting file {out_file_path}')
        os.remove(path=out_file_path)



# %%

@catch_error()
def iterate_over_all_pershing(remote_path:str, zip_file_name:str=None):
    """
    Main function to iterate over all the files
    """
    if not remote_path:
        logger.warning('Empty Remote Path - skipping')
        return

    config_is_zip_file = config.is_zip_file.upper() == 'TRUE'

    for file_name in os.listdir(remote_path):
        file_path = os.path.join(remote_path, file_name)
        if not os.path.isfile(file_path): continue

        file_name_noext, file_ext = os.path.splitext(file_name)

        if file_ext.lower() == '.zip':
            if config_is_zip_file:
                for name_like in config.file_name_like_list:
                    if name_like.upper() in file_name.upper():
                        with tempfile.TemporaryDirectory(dir=config.temporary_folder_path) as tmpdir:
                            extract_dir = tmpdir
                            logger.info(f'Extracting {file_path} to {extract_dir}')
                            shutil.unpack_archive(filename=file_path, extract_dir=extract_dir, format='zip')
                            if not zip_file_name: # maintain original zip_file_name
                                zip_file_name = file_path
                            iterate_over_all_pershing(remote_path=extract_dir, zip_file_name=zip_file_name)
                        break
            continue

        if (config_is_zip_file and zip_file_name) or (not config_is_zip_file):
            for name_like in config.file_name_like_list:
                if (config_is_zip_file and name_like.upper() in zip_file_name.upper()) or (not config_is_zip_file and name_like.upper() in file_name.upper()):
                    process_single_pershing(file_path=file_path)
                    break



# %%

def find_latest_remote_path():
    """
    Deal with path variability, to find latest path
    """
    remote_path = config.remote_path

    if config.firm_name.upper() in ['IFX'] and environment.is_prod:
        # /opt/EDIP/remote/APP01/ftproot/ifxftp1/2024-01-26
        folder_paths = {}
        for folder_name in os.listdir(config.remote_path):
            folder_path = os.path.join(config.remote_path, folder_name)
            if os.path.isdir(folder_path):
                try:
                    folder_date = datetime.strptime(folder_name, r'%Y-%m-%d')
                    folder_paths[folder_date] = folder_path
                except Exception as e:
                    logger(f'Invalid Folder name, should be date folder: {folder_path}')
        remote_path = folder_path[max(folder_paths)]

    return remote_path



remote_path = find_latest_remote_path()
iterate_over_all_pershing(remote_path=remote_path)




# %% Close Connections / End Program

logger.mark_ending()



# %%


