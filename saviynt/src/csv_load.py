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

import pyodbc, subprocess, tempfile, shutil, re
from typing import List
from datetime import datetime

from saviynt_modules.logger import logger, catch_error
from saviynt_modules.connections import Connection
from saviynt_modules.settings import Config, normalize_name



# %% Parameters

args |=  {
    }



# %% Get Config

config = Config(args=args)

target_connection = Connection.from_config(config=config, prefix='target')



# %%

@catch_error()
def get_separator(header_string:str):
    """
    Find out what separator is used in the file header
    """
    separators = ['!#!#', '|', '\t']
    delimiter = ','
    for s in separators:
        if s in header_string:
            delimiter = s
            break
    return delimiter



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
def execute_sql_queries(sql_list:List, connection:Connection):
    """
    Execute given list of SQL queries
    """
    outputs = []
    with pyodbc.connect(connection.conn_str_sql_server()) as conn:
        cursor = conn.cursor()
        for sql_str in sql_list:
            result = cursor.execute(sql_str)
            outputs.append(result)
        conn.commit()
    return outputs



# %%

@catch_error()
def create_or_truncate_table(table_name_with_schema:str, columns:List, connection:Connection, truncate:bool=False):
    """
    Create a new table is not exists otherwise optionally truncate
    """
    columns_sql = ",\n".join([f"[{column}] NVARCHAR(MAX)" for column in columns])
    create_table_sql = f'''
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(CONCAT_WS('.', table_schema, table_name)) = LOWER('{table_name_with_schema}'))
    BEGIN
        CREATE TABLE {table_name_with_schema} (
            {columns_sql}
        );
    END
    '''
    sql_list = [create_table_sql]

    if truncate:
        truncate_table_sql = f'TRUNCATE TABLE {table_name_with_schema}'
        sql_list.append(truncate_table_sql)

    outputs = execute_sql_queries(sql_list=sql_list, connection=connection)



# %%

catch_error()
def run_process(command:str):
    """
    Run command line process. Returns None if error.
    """
    encoding = 'UTF-8'
    command_regex = r'\r?\n' # remove line endings
    command = re.sub(command_regex, ' ', command) # remove line endings

    process = subprocess.Popen(
        args = command,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        shell = True,
        )

    stdout, stderr = '', ''

    k = 0
    while k<2:
        out = process.stdout.read().decode(encoding).rstrip('\n')
        err = process.stderr.read().decode(encoding).rstrip('\n')

        if out: print(out)
        if err: print(err)
        stdout += out
        stderr += err

        if process.poll() is not None:
            k += 1

    #stdout, stderr = process.communicate()
    #stdout, stderr = stdout.decode(encoding), stderr.decode(encoding)

    if process.returncode != 0:
        logger.error(f'Error in running command: {command}')
        return None

    return stdout



# %%

@catch_error()
def bcp_to_sql_server_csv(file_path:str, connection:Connection, table_name_with_schema:str, delimiter:str=','):
    """
    Send CSV/PSV data file to SQL Server using bcp tool
    if no username or password given, then use trusted connection
    """
    authentication_str = '-T' if connection.is_trusted_connection() else f'-U {connection.username} -P {connection.password}'

    bcp_str = f"""
        bcp {connection.database}.{table_name_with_schema}
        in "{file_path}"
        -S {connection.server}
        {authentication_str}
        -c
        -t "{delimiter}"
        -F 2
        """

    stdout = run_process(command=bcp_str)
    if not stdout: return

    try:
        rows_copied = int(stdout[:stdout.find(' rows copied.')].split('\n')[-1]) - 1 # remove header row
    except Exception as e:
        logger.error(f'Exceptin on extracting rows_copied: {str(e)}')
        return

    logger.info(f'Server: {connection.server}; Table: {connection.database}.{table_name_with_schema}; Rows copied: {rows_copied}; File: {file_path}')
    return rows_copied



# %%

@catch_error()
def allowed_file_extensions(config:Config):
    """
    Retrieve Allowed file extensions
    """
    if not hasattr(config, 'allowed_file_extensions'): return

    if isinstance(config.allowed_file_extensions, str):
        ext_regex = r'[^a-zA-Z0-9]'
        ext_list = config.allowed_file_extensions.lower().split(',')
        ext_list = [re.sub(ext_regex, '', e) for e in ext_list]
        config.allowed_file_extensions = ['.'+e for e in ext_list if e]
    elif isinstance(config.allowed_file_extensions, List):
        pass
    else:
        raise ValueError(f'Error in config.allowed_file_extensions:{config.allowed_file_extensions} Invalid Type: {type(config.allowed_file_extensions)}')

    return config.allowed_file_extensions



# %%

def normalize_table_name(table_name_raw:str, config:Config):
    """
    Normalize table_name, add prefix (if any) and schema
    """
    if hasattr(config, 'table_prefix') and config.table_prefix.strip():
        prefix = config.table_prefix.strip().lower() + '_'
        if not table_name_raw.lower().startswith(prefix):
            table_name_raw = prefix + table_name_raw.lower()

    table_name_with_schema = normalize_name(name=config.target_schema) + '.' + normalize_name(name=table_name_raw)
    return table_name_with_schema.lower()



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
        date_of_data = logger.execution_date.start

    table_name_with_schema = normalize_table_name(table_name_raw=table_name_raw, config=config)

    return table_name_with_schema, date_of_data



# %%

@catch_error()
def get_file_meta_csv(file_path:str, config:Config, zip_file_path:str=None):
    """
    Extract file metadata
    """
    columns, delimiter = get_csv_file_columns_and_delimiter(file_path=file_path)
    if not columns: return

    table_name_with_schema, date_of_data = extract_table_name_and_date_from_file_name(file_path=file_path, config=config, zip_file_path=zip_file_path)
    if not table_name_with_schema: return

    is_full_load = True # default value
    if hasattr(config, 'is_full_load'):
        is_full_load = config.is_full_load.upper() == 'TRUE'

    file_meta = {
        'file_name': os.path.basename(file_path),
        'file_path': file_path,
        'zip_file_name': os.path.basename(zip_file_path) if zip_file_path else None,
        'zip_file_path': zip_file_path,
        'columns': columns,
        'delimiter': delimiter,
        'table_name_with_schema': table_name_with_schema,
        'database_name': config.target_database.strip(),
        'file_type': 'csv',
        'is_full_load': is_full_load,
        'date_of_data': date_of_data,
        'source_server': None,
        'source_database': None,
        'source_table_name_with_schema': None,
    }

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

    create_or_truncate_table(table_name_with_schema=file_meta['table_name_with_schema'], columns=file_meta['columns'], connection=target_connection, truncate=True)

    rows_copied = bcp_to_sql_server_csv(file_path=file_path, connection=target_connection, table_name_with_schema=file_meta['table_name_with_schema'], delimiter=file_meta['delimiter'])
    if rows_copied is None: return
    file_meta['rows_copied'] = rows_copied

    logger.info(f'Finished processing file: {file_path}')



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

            csv_file_to_sql_server(
                file_path = file_path,
                target_connection = target_connection,
                config = config,
            )



# %%

recursive_migrate_all_files(source_path=config.source_path, config=config, target_connection=target_connection)



# %% Close Connections / End Program

logger.mark_execution_end()



# %%


