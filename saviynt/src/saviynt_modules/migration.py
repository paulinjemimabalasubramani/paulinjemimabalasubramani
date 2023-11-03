"""
Module to handle high level migration tasks, also record metrics / metadata about migration

"""

# %% Import Libraries

import tempfile, shutil, os
from typing import Callable

from .logger import logger, catch_error
from .settings import Config
from .filemeta import get_file_meta_csv
from .sqlserver import migrate_file_to_sql_table



# %%

@catch_error()
def recursive_migrate_all_files(source_path:str, config:Config, fn_migrate_file:Callable, zip_file_path:str=None):
    """
    Migrate all files to the database. Unzip archive files and migrate contents as well.
    """
    selected_file_paths = []

    if os.path.isdir(source_path):
        for root, dirs, files in os.walk(source_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                selected_file_paths.append(file_path)
    else:
        selected_file_paths.append(source_path)

    selected_file_paths = sorted(selected_file_paths)

    for root, dirs, files in os.walk(source_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=config.temporary_folder_path) as tmpdir:
                    extract_dir = tmpdir
                    logger.info(f'Extracting {file_path} to {extract_dir}')
                    shutil.unpack_archive(filename=file_path, extract_dir=extract_dir, format='zip')
                    zip_file_path = zip_file_path if zip_file_path else file_path # to keep original zip file path, rather than the last zip file path
                    recursive_migrate_all_files(
                        source_path = extract_dir,
                        config = config,
                        zip_file_path = zip_file_path,
                        )
                continue

            file_meta = fn_migrate_file(
                file_path = file_path,
                config = config,
                zip_file_path = zip_file_path,
            )



# %% 

@catch_error()
def migrate_csv_file_to_sql_server(file_path:str, config:Config, zip_file_path:str=None):
    """
    Create SQL Server table with csv file contents
    """
    logger.info(f'Processing file: {file_path}')

    file_meta = get_file_meta_csv(file_path=file_path, config=config, zip_file_path=zip_file_path)

    migrate_file_to_sql_table(file_meta=file_meta, connection=config.target_connection, config=config)

    logger.info(f'Finished processing file: {file_path}')
    return file_meta




# %%


