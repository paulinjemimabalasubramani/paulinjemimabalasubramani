"""
Module to handle high level migration tasks, also record metrics / metadata about migration

"""

# %% Import Libraries

import tempfile, os
from typing import List, Dict
from zipfile import ZipFile

from .logger import logger, catch_error
from .settings import Config
from .connections import Connection
from .common import to_sql_value
from .filemeta import get_file_meta, FileMeta
from .sqlserver import migrate_file_to_sql_table, create_or_truncate_sql_table, execute_sql_queries, sql_table_exists



# %%

def get_config(args:dict):
    """
    Create Config object with args
    Add ELT and Target connections to config object
    """
    config = Config(args=args)

    for connection_prefix in ['elt', 'target']:
        config.add_connection_from_config(prefix=connection_prefix, Connection=Connection)

    return config



# %%

@catch_error()
def add_file_meta_to_load_history(file_meta:FileMeta, config:Config):
    """
    Add File Meta data to Load History table
    """
    load_history_table = config.elt_table_load_history
    elt_columns = file_meta.get_elt_load_history_columns()
    elt_connection = config.elt_connection
    logger.info(f'Updating load history for {load_history_table}')

    create_or_truncate_sql_table(table_name_with_schema=load_history_table, columns=elt_columns, connection=elt_connection, truncate=False)

    elt_columns.pop('id')
    elt_values_dict = file_meta.get_elt_values_sql()
    elt_values_sql = ','.join([elt_values_dict[c] for c in elt_columns])

    insert_history_sql = f'''INSERT INTO {load_history_table} ({','.join(elt_columns)}) VALUES ({elt_values_sql})'''
    execute_sql_queries(sql_list=[insert_history_sql], connection=elt_connection)



# %%

@catch_error()
def get_selected_file_paths(file_paths:str|List):
    """
    Get file paths as a list
    """
    selected_file_paths = []

    if isinstance(file_paths, str):
        if os.path.isdir(file_paths):
            for root, dirs, files in os.walk(file_paths):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    selected_file_paths.append(file_path)
        else:
            selected_file_paths.append(file_paths)
        selected_file_paths = sorted(selected_file_paths)
    else:
        selected_file_paths = file_paths

    return selected_file_paths



# %%

@catch_error()
def file_meta_exists_in_history(config:Config, file_meta:FileMeta=None, **kwargs):
    """
    Check whether to migrate the file or not
    False return means to ingest the file | True return means to skip the file
    """
    if not sql_table_exists(table_name_with_schema=config.elt_table_load_history, connection=config.elt_connection):
        return False

    if kwargs: # custom check
        check_dict = {c:to_sql_value(v) for c, v in kwargs.items()}
    elif file_meta: # detailed check
        check_list = ['table_name_with_schema', 'database_name', 'server_name', 'date_of_data', 'is_full_load']
        elt_values = file_meta.get_elt_values_sql()
        check_dict = {c:elt_values[c] for c in check_list}
    else:
        raise ValueError('Either file_meta or **kwargs should be given')

    check_dict_filter = ' AND '.join([f'{c} IS NULL' if v.upper().strip()=='NULL' or v.strip()=='' or v is None else f'{c}={v}' for c, v in check_dict.items()])

    exists_sql = f'''
        SELECT COUNT(*) AS CNT
        FROM {config.elt_table_load_history}
        WHERE {check_dict_filter}
        '''

    output = execute_sql_queries(sql_list=[exists_sql], connection=config.elt_connection)
    return output[0][0][0]>0



# %%

@catch_error()
def migrate_single_file_to_sql_server(file_type:str, file_path:str, config:Config, zip_file_path:str=None):
    """
    Main function to migrate single file to SQL Server
    """
    logger.info(f'Processing file: {file_path}')

    file_meta = get_file_meta(file_type=file_type, file_path=file_path, config=config, zip_file_path=zip_file_path)
    if not file_meta:
        logger.warning(f'File Meta could not be processed, skipping {file_path}')
        return

    if file_meta_exists_in_history(config=config, file_meta=file_meta):
        logger.info(f'The file has been loaded previously, skipping {file_path}')
        return

    migrate_file_to_sql_table(file_meta=file_meta, connection=config.target_connection, config=config)
    add_file_meta_to_load_history(file_meta=file_meta, config=config)

    logger.info(f'Finished processing file: {file_path}')
    return file_meta



# %%

@catch_error()
def recursive_migrate_all_files(file_type:str, file_paths:str|List, config:Config, zip_file_path:str=None):
    """
    Migrate all files to the database. Unzip archive files and migrate contents as well.
    """
    selected_file_paths = get_selected_file_paths(file_paths=file_paths)

    for file_path in selected_file_paths:
        if os.path.isdir(file_path):
            logger.warning(f'{file_path} is not a file path (it is a folder), skipping.')
            continue

        file_name = os.path.basename(file_path)
        file_name_noext, file_ext = os.path.splitext(file_name)

        if file_ext.lower() == '.zip':
            zip_file_path = zip_file_path if zip_file_path else file_path # to keep original zip file path, rather than the last zip file path

            with ZipFile(file_path, 'r') as zipobj:
                zipinfo_list = zipobj.infolist()
                for zipinfo in zipinfo_list:
                    if zipinfo.is_dir(): continue

                    if file_meta_exists_in_history(config=config, file_type=file_type, file_name=os.path.basename(zipinfo.filename), zip_file_path=zip_file_path):
                        logger.info(f'The file already has been loaded or the file is not used, skipping file_type={file_type}, filename={os.path.basename(zipinfo.filename)}, zip_file_path={zip_file_path}')
                        continue

                    with tempfile.TemporaryDirectory(dir=config.temporary_folder_path) as tmpdir:
                        logger.info(f'Extracting {zipinfo.filename} from {file_path} to {tmpdir}')
                        zipobj.extract(member=zipinfo, path=tmpdir)
                        new_file_path = os.path.join(tmpdir, zipinfo.filename)

                        recursive_migrate_all_files(file_type=file_type, file_paths=new_file_path, config=config, zip_file_path=zip_file_path)
            continue

        file_meta = migrate_single_file_to_sql_server(file_type=file_type, file_path=file_path, config=config, zip_file_path=zip_file_path)



# %%


