__description__ = """
Load data to SQL Server using bcp tool

bcp <databse_name>.<schema_name>.<table_name> in "<file_path>" -S <server_name>.<dns_suffix> -U <username> -P <password> -c -t "|" -F 2

bcp SaviyntIntegration.dbo.envestnet_hierarchy_firm in "C:/myworkdir/data/envestnet_v35_processed/hierarchy_firm_20231009.txt" -S DW1SQLDATA01.ibddomain.net -T -c -t "|" -F 2

"""


# %% Strat Logging

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
from saviynt_modules.logger import logger, catch_error
from saviynt_modules.connections import Connection
from saviynt_modules.settings import Config, normalize_name
from typing import List



# %% Parameters

args |=  {
    }



# %% Get Config

config = Config(args=args)



# %%

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
    process = subprocess.Popen(
        args = command,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        )

    out, err = process.communicate()
    out, err = out.decode(encoding), err.decode(encoding)
    logger.info(out)

    if process.returncode != 0:
        logger.info(out)
        logger.error(out)
        logger.error(f'Error in running command: {command}')
        return None

    return out



# %%

@catch_error()
def bcp_to_sql_server(file_path:str, connection:Connection, table_name_with_schema:str, delimiter:str=','):
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

    results = run_process(command=bcp_str)
    print(results)
    return results



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

@catch_error()
def get_file_meta(file_path:str, config:Config):
    """
    Extract file metadata
    """
    columns, delimiter = get_csv_file_columns_and_delimiter(file_path=file_path)
    if not columns: return

    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()

    allowed_file_ext = allowed_file_extensions(config=config)
    if allowed_file_ext and file_ext.lower() not in allowed_file_ext:
        logger.warning(f'Only {allowed_file_ext} extensions are allowed: {file_path}')
        return

    table_name_with_schema = 

    file_meta = {
        'file_path': file_path,
        'columns': columns,
        'delimiter': delimiter,
        'table_name_with_schema': table_name_with_schema,
    }



# %% 

@catch_error()
def csv_file_to_sql_server(file_path:str, connection:Connection, config:Config):
    """
    Create SQL Server table with csv file contents
    """
    file_meta = get_file_meta(file_path=file_path, config=config)
    if not file_meta:
        logger.warning(f'Invalid file, NOT processing: {file_path}')
        return

    create_or_truncate_table(table_name_with_schema=file_meta['table_name_with_schema'], columns=file_meta['columns'], connection=connection, truncate=True)
    bcp_to_sql_server(file_path=file_path, connection=connection, table_name_with_schema=file_meta['table_name_with_schema'], delimiter=file_meta['delimiter'])



# %%

@catch_error()
def recursive_migrate_all_files(source_path:str, config:Config, connection, zip_file_path:str=None):
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
                        connection = connection,
                        zip_file_path = zip_file_path,
                        )
                continue

            csv_file_to_sql_server(
                file_path = file_path,
                connection = connection,
            )



# %%

recursive_migrate_all_files(source_path=config.source_path, config=config, connection=Connection)



# %% Close Connections / End Program

logger.mark_execution_end()



# %%


