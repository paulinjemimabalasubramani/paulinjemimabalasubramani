__description__ = """
Load data to SQL Server using bcp tool

bcp <databse_name>.<schema_name>.<table_name> in "<file_path>" -S <server_name>.<dns_suffix> -U <username> -P <password> -c -t "|" -F 2

bcp SaviyntIntegration.dbo.envestnet_hierarchy_firm in "C:/myworkdir/data/envestnet_v35_processed/hierarchy_firm_20231009.txt" -S DW1SQLDATA01.ibddomain.net -T -c -t "|" -F 2

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

import tempfile, shutil, re
from typing import List
from datetime import datetime

from saviynt_modules.logger import logger, catch_error
from saviynt_modules.connections import Connection
from saviynt_modules.settings import Config, normalize_name
from saviynt_modules.migration import FileMeta, allowed_file_extensions, bcp_to_sql_server_csv, create_or_truncate_sql_table, normalize_table_name,\
    execute_sql_queries, staging_schema
from saviynt_modules.common import get_separator



# %% Parameters

args |=  {
    }



# %% Get Config

config = Config(args=args)

target_connection = Connection.from_config(config=config, prefix='target')



# %%

@catch_error()
def get_csv_file_columns_and_delimiter(file_path:str):
    """
    Get list of columns and delimiter used for csv file
    """
    # Read the header from the CSV file
    with open(file=file_path, mode='rt', encoding='utf-8-sig', errors='ignore') as f:
        HEADER = f.readline()

    delimiter = get_separator(header_string=HEADER)

    columns = HEADER.split(delimiter)
    columns = [normalize_name(c) for c in columns]
    return columns, delimiter



# %%

@catch_error()
def extract_table_name_and_date_from_file_name(file_path:str, config:Config, zip_file_path:str=None):
    """
    Extract table_name and date of data from file_name
    """
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()

    allowed_file_ext = allowed_file_extensions(config=config)
    if allowed_file_ext and file_ext.lower() not in allowed_file_ext:
        logger.warning(f'Only {allowed_file_ext} extensions are allowed: {file_path}')
        return None, None

    if hasattr(config, 'file_name_regex'):
        match = re.search(config.file_name_regex, file_name_noext)
        if match:
            try:
                table_name_raw = match.group(1)
                if hasattr(config, 'file_date_format'):
                    file_date_str = match.group(2) # Assuming that date will be 2nd
            except Exception as e:
                logger.warning(f'Invalid Match, could not retrieve table_name or file_date from regex pattern: {file_path}. {str(e)}')
                return None, None
        else:
            logger.warning(f'Invalid Match, Could not find date stamp for the file or invalid file name: {file_path}')
            return None, None
    else:
        if hasattr(config, 'file_date_format'):
            date_loc = -file_name_noext[::-1].find('_')
            if date_loc>=0:
                logger.warning(f'Could not find date stamp for the file or invalid file name: {file_path}')
                return None, None
            file_date_str = file_name_noext[date_loc:]
            table_name_raw = file_name_noext[:date_loc-1]
        else:
            table_name_raw = file_name_noext

    if hasattr(config, 'file_date_format'):
        date_of_data = datetime.strptime(file_date_str, config.file_date_format)
    else:
        date_of_data = logger.run_date.start

    table_name_with_schema = normalize_table_name(table_name_raw=table_name_raw, config=config)

    return table_name_with_schema, date_of_data



# %%

@catch_error()
def get_file_meta_csv(file_path:str, config:Config, zip_file_path:str=None):
    """
    Extract file metadata
    """
    file_type = 'csv'

    columns, delimiter = get_csv_file_columns_and_delimiter(file_path=file_path)
    if not columns: return

    table_name_with_schema, date_of_data = extract_table_name_and_date_from_file_name(file_path=file_path, config=config, zip_file_path=zip_file_path)
    if not table_name_with_schema: return

    file_meta = FileMeta(
        table_name_with_schema = table_name_with_schema,
        file_path = file_path,
        zip_file_path = zip_file_path,
        columns = columns,
        delimiter = delimiter,
        file_type = file_type,
        date_of_data = date_of_data,
    )

    file_meta.add_config(config=config)

    return file_meta



# %% 

@catch_error()
def csv_file_to_sql_server(file_path:str, target_connection:Connection, config:Config):
    """
    Create SQL Server table with csv file contents
    """
    logger.info(f'Processing file: {file_path}')

    file_meta = get_file_meta_csv(file_path=file_path, config=config)
    if not file_meta:
        logger.warning(f'Invalid file, NOT processing: {file_path}')
        return

    create_or_truncate_sql_table(table_name_with_schema=file_meta['table_name_with_schema'], columns=file_meta['columns'], connection=target_connection, truncate=True)

    file_meta.rows_copied = bcp_to_sql_server_csv(file_path=file_path, connection=target_connection, table_name_with_schema=file_meta['table_name_with_schema'], delimiter=file_meta['delimiter'])
    if file_meta.rows_copied is None: return

    logger.info(f'Finished processing file: {file_path}')
    return file_meta




# %%


file_path = r'C:\myworkdir\data\envestnet_v35_processed\codes_account_status_20231009.txt'

file_meta = get_file_meta_csv(file_path=file_path, config=config)
create_or_truncate_sql_table(table_name_with_schema=file_meta.table_name_with_schema, columns=file_meta.columns, connection=target_connection, truncate=True)
file_meta.rows_copied = bcp_to_sql_server_csv(file_path=file_path, connection=target_connection, table_name_with_schema=file_meta.table_name_with_schema, delimiter=file_meta.delimiter)

print('Rows Copied:', file_meta.rows_copied)


 # %%

staging_table =  staging_schema + '.' + file_meta.table_name_with_schema.split('.')[1]

create_or_truncate_sql_table(table_name_with_schema=staging_table, columns=file_meta.columns, connection=target_connection, truncate=True)
file_meta.rows_copied = bcp_to_sql_server_csv(file_path=file_path, connection=target_connection, table_name_with_schema=staging_table, delimiter=file_meta.delimiter)




# %%

@catch_error()
def recursive_migrate_all_files(source_path:str, config:Config, target_connection:Connection, zip_file_path:str=None):
    """
    Migrate all files to the database. Unzip archive files and migrate contents as well.
    """
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
                        target_connection = target_connection,
                        zip_file_path = zip_file_path,
                        )
                continue

            file_meta = csv_file_to_sql_server(
                file_path = file_path,
                target_connection = target_connection,
                config = config,
            )



# %%

recursive_migrate_all_files(source_path=config.source_path, config=config, target_connection=target_connection)



# %% Close Connections / End Program

logger.mark_run_end()



# %%


