# %%

__description__ = """

Copy Envestnet files for Saviynt

"""


# %% Start Logging

import os
from saviynt_modules.logger import catch_error, environment, logger
logger.set_logger(app_name=os.path.basename(__file__))



# %% Parse Arguments

if environment.is_prod:
    import argparse

    parser = argparse.ArgumentParser(description=__description__)

    parser.add_argument('--pipeline_key', '--pk', help='pipeline_key value for getting pipeline settings', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipeline_key': 'saviynt_envestnet',
        }



# %% Import Libraries

import re, shutil
from datetime import datetime
from zipfile import ZipFile
from collections import defaultdict

from saviynt_modules.settings import Config, get_csv_rows, normalize_name
from saviynt_modules.common import remove_last_line_from_file
from saviynt_modules.connections import Connection
from saviynt_modules.migration import migrate_csv_file_to_sql_server



# %% Parameters

args |=  {
    'final_file_ext': '.txt',
    'last_line_text_seek': 'T|',
    }



# %% Get Config

config = Config(args=args)
config.add_target_connection(Connection=Connection)



# %%

@catch_error()
def get_envestnet_schema():
    """
    Get copy schema - i.e. list of file names and versions to copy
    """
    schema1 = defaultdict(list)
    for row in get_csv_rows(csv_file_path=config.schema_path):
        #table_name, record_type, field, sub_table_name
        table_name = normalize_name(row['table_name'])
        sub_table_name = normalize_name(row['sub_table_name'])
        field = normalize_name(row['field'])
        record_type = row['record_type'].strip().lower()

        for x in schema1[(table_name, record_type)]:
            if x[0]==field:
                raise ValueError(f'Duplicate name found for table_name={table_name} field={field}')

        schema1[(table_name, record_type)].append((field, sub_table_name))

    schema2 = defaultdict(list)
    table_list = []
    for (table_name, record_type), val in schema1.items():
        table_list.append(table_name)
        sub_table_name_x = ''
        for (field, sub_table_name) in val:
            if sub_table_name:
                if sub_table_name_x and sub_table_name!=sub_table_name_x:
                    raise ValueError(f'Two different sub-table names are given in schema file: table_name = {table_name}, record_type={record_type}, field={field}, sub_table_name1={sub_table_name}, sub_table_name2={sub_table_name_x}')
                else:
                    sub_table_name_x = sub_table_name

        if not sub_table_name_x:
            sub_table_name_x = normalize_name(record_type)

        if sub_table_name_x:
            final_table_name = table_name + '_' + sub_table_name_x
        else:
            final_table_name = table_name

        for (field, sub_table_name) in val:
            if field in schema2[(table_name, record_type, final_table_name)]:
                raise ValueError(f'Duplicate field found in (table_name, record_type, final_table_name) = {(table_name, record_type, final_table_name)}, field={field}')
            else:
                schema2[(table_name, record_type, final_table_name)].append(field)

    table_list = list(set(table_list))
    return schema2, table_list



envestnet_schema, table_list = get_envestnet_schema()

table_list = ['hierarchy', 'codes'] # Override table_list to have only the one needed for Saviynt



# %%

@catch_error()
def get_zip_files():
    """
    Get the latest zip files to extract data, based on latest date
    """
    zip_files = []
    zip_dates = []

    for file_name in os.listdir(config.remote_path):
        remote_file_path = os.path.join(config.remote_path, file_name)
        if os.path.isfile(remote_file_path):
            file_name_noext, file_ext = os.path.splitext(file_name)

            date_str = file_name_noext[-8:]
            if file_ext.lower()!='.zip' or not date_str.isdigit() or 'V35' not in file_name_noext:
                logger.info(f'Not a valid Envestnet V35 ZIP file, skipping: {remote_file_path}')
                continue

            try:
                date_val = datetime.strptime(date_str, r'%Y%m%d')
            except:
                logger.warning(f'Not a valid zip file date: {remote_file_path}')
                continue

            zip_files.append(remote_file_path)
            zip_dates.append(date_val)

    maxdate = max(zip_dates)
    zip_files = [zip_files[i] for i, x in enumerate(zip_dates) if x == maxdate]
    return zip_files



zip_files = get_zip_files()



# %%

@catch_error()
def parse_envestnet_file_name(file_name, zip_name):
    """
    Parse Envestnet file name and return clean names
    """
    file_name_noext, file_ext = os.path.splitext(os.path.basename(file_name))
    date_str = file_name_noext[-8:]
    if file_ext.lower()!='.psv' or not date_str.isdigit(): 
        logger.warning(f'Not an valid file: {file_name} in zip archive {zip_name} -> skipping file')
        return None, None

    try:
        _ = datetime.strptime(date_str, r'%Y%m%d')
    except:
        logger.warning(f'Invalid file date: {file_name} in zip archive {zip_name} -> skipping file')
        return None, None

    file_name_noext = re.sub('_+', '_', file_name_noext[:-8])[:-1].lower()
    file_prefix = 'AG_V35_'
    if file_name_noext[:len(file_prefix)].upper() != file_prefix.upper():
        logger.warning(f'File does not start with "{file_prefix}": {file_name} in zip archive {zip_name} -> skipping file')
        return None, None

    file_name_split = file_name_noext.split('_')

    if len(file_name_split)!=3:
        logger.warning(f'Cannot parse file name: {file_name} in zip archive {zip_name} -> skipping file')
        return None, None

    table_name = file_name_split[2].lower()
    file_version = file_name_split[1].upper()

    return table_name, file_version



# %%

@catch_error()
def get_table_schemas(table_name:str):
    """
    Retrieve schemas for given table_name
    """
    table_schemas = dict()
    for (table_name1, record_type, final_table_name), field_list in envestnet_schema.items():
        if table_name1.lower()==table_name.lower():
            table_schemas[(final_table_name, record_type)] = field_list
    return table_schemas



# %%

@catch_error()
def get_field_list_from_table_schema(table_schemas:dict, record_type:str=''):
    """
    Get list of fields (columns) for a given record type - for creating sub-tables
    """
    field_list = []
    final_table_name = ''
    for (final_table_name, record_type1), field_list in table_schemas.items():
        if record_type1.lower() == record_type.lower():
            break
    if not field_list or not final_table_name or record_type1.lower() != record_type.lower():
        raise ValueError(f'Record type "{record_type}" does not exist in Table Schemas {table_schemas}\nfinal_table_name={final_table_name}\nfield_list={field_list}')
    return final_table_name, field_list



# %%

@catch_error()
def process_psv_file(file_path:str):
    """
    Process single unzipped PSV file
    """
    with open(file=file_path, mode='rt', encoding='utf-8-sig', errors='ignore') as f:
        HEADER = f.readline()

    if HEADER[:2] != 'H|':
        logger.warning(f'Header is not found in 1st line inside the file: {file_path}')
        return

    HEADER = HEADER.split(sep='|')
    try:
        table_name = normalize_name(HEADER[1])
        file_date = HEADER[2]
    except Exception as e:
        logger.warning(f'Error occurred while reading file HEADER info: {file_path}')
        return

    try:
        _ = datetime.strptime(file_date, r'%Y%m%d')
    except:
        logger.warning(f'Invalid file date: {file_path}')
        return

    table_schemas = get_table_schemas(table_name=table_name)

    if len(table_schemas)==0:
        logger.warning(f'Table schema is not found for table_name="{table_name}" file={file_path}')
        return

    destination_paths = []
    logger.info(f'Processing file {file_path}')
    with open(file=file_path, mode='rt', encoding='utf-8-sig', errors='ignore') as fsource:
        _ = fsource.readline() # discard header line
        if len(table_schemas)==1: # does not have sub-tables
            final_table_name, field_list = get_field_list_from_table_schema(table_schemas=table_schemas, record_type='')
            destination_path = os.path.join(os.path.dirname(file_path), final_table_name+'_'+file_date+config.final_file_ext)
            destination_paths.append(destination_path)
            logger.info(f'Creating file {destination_path}')
            with open(file=destination_path, mode='wt', encoding='utf-8-sig') as fdest:
                fdest.write('|'.join(field_list)+'\n')
                shutil.copyfileobj(fsource, fdest)

            remove_last_line_from_file(file_path=destination_path, last_line_text_seek=config.last_line_text_seek)

        else: # has sub-tables
            record_types = dict()
            for line in fsource:
                if line.startswith(config.last_line_text_seek):
                    break

                record_type = line[:line.find('|')].strip().lower() # take first column as record_type
                if not record_type:
                    raise ValueError(f'Invalid line {line}')

                if record_type not in record_types:
                    final_table_name, field_list = get_field_list_from_table_schema(table_schemas=table_schemas, record_type=record_type)
                    destination_path = os.path.join(os.path.dirname(file_path), final_table_name+'_'+file_date+config.final_file_ext)
                    destination_paths.append(destination_path)
                    logger.info(f'Creating file {destination_path}')
                    fdest = open(file=destination_path, mode='wt', encoding='utf-8-sig')
                    fdest.write('|'.join(field_list)+'\n')
                    record_types[record_type] = fdest

                record_types[record_type].write(line)

            for record_type, fdest in record_types.items():
                fdest.close()

    logger.info(f'Deleting file {file_path}')
    os.remove(file_path)

    for destination_path in destination_paths:
        migrate_csv_file_to_sql_server(file_path=destination_path, config=config)
        logger.info(f'Deleting file {destination_path}')
        os.remove(destination_path)



# %%

@catch_error()
def extract_zip_files():
    """
    Extract selected zip files according to copy schema / file version.
    """
    for zip_file in zip_files:
        with ZipFile(zip_file, 'r') as zipobj:
            zipinfo_list = zipobj.infolist()
            for zipinfo in zipinfo_list:
                if zipinfo.is_dir(): continue

                table_name, file_version = parse_envestnet_file_name(file_name=zipinfo.filename, zip_name=zip_file)
                if not table_name: continue

                if table_name.lower() not in table_list:
                    logger.warning(f'File name does not exist in Envestnet schema, unknown file name: {zipinfo.filename} in zip archive {zip_file} -> skipping file')
                    continue

                logger.info(f'Extracting {zipinfo.filename} from {zip_file} to {config.source_path}')
                zipobj.extract(member=zipinfo, path=config.source_path)
                process_psv_file(file_path=os.path.join(config.source_path, zipinfo.filename))



extract_zip_files()



# %% Close Connections / End Program

logger.mark_run_end()



# %%


