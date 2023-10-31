"""
Module to handle common migration tasks

"""


# %%

import os, re, pyodbc
from dataclasses import dataclass
from datetime import datetime
from typing import List

from .settings import Config, normalize_name
from .logger import logger, catch_error
from .connections import Connection
from .common import run_process, remove_square_parenthesis


# %% Parameters

default_data_type = 'NVARCHAR(MAX)'
staging_schema = 'stg'



# %%

@dataclass
class FileMeta:
    """
    Store common File Meta data
    """
    table_name_with_schema:str
    file_path:str
    zip_file_path:str = None
    columns:str = None
    delimiter:str = None
    database_name:str = None
    server_name:str = None
    file_type:str = None
    is_full_load:bool = None
    date_of_data:datetime = None
    source_server:str = None
    source_database:str = None
    source_table_name_with_schema:str = None
    pipeline_key:str = None
    additional_info:str = None
    rows_copied:int = None


    def __post_init__(self):
        """
        Runs after __init__
        """
        self.file_name = os.path.basename(self.file_path)
        self.zip_file_name = os.path.basename(self.zip_file_path) if self.zip_file_path else None
        self.run_date = logger.run_date.start


    @catch_error()
    def add_config(self, config:Config):
        """
        Add metadata from config and other default values
        """
        if self.database_name is None and hasattr(config, 'target_database'): self.database_name = config.target_database
        if self.server_name is None  and hasattr(config, 'target_server'): self.server_name = config.target_server
        if self.pipeline_key is None : self.pipeline_key = config.pipeline_key
        if self.date_of_data is None : self.date_of_data = logger.run_date.start
        if self.is_full_load is None and hasattr(config, 'is_full_load'): self.is_full_load = config.is_full_load.upper() == 'TRUE'



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
        rows_copied = int(stdout[:stdout.find(' rows copied.')].split('\n')[-1])
    except Exception as e:
        logger.error(f'Exceptin on extracting rows_copied: {str(e)}')
        return

    logger.info(f'Server: {connection.server}; Table: {connection.database}.{table_name_with_schema}; Rows copied: {rows_copied}; File: {file_path}')
    return rows_copied



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
def execute_sql_queries(sql_list:List, connection:Connection):
    """
    Execute given list of SQL queries
    """
    outputs = []
    with pyodbc.connect(connection.conn_str_sql_server()) as conn:
        cursor = conn.cursor()
        for sql_str in sql_list:
            cursor.execute(sql_str)
            if cursor.description:
                results = cursor.fetchall()
            else:
                results = ['DML statement (success)']
            outputs.append(results)
        conn.commit()
    return outputs



# %%

@catch_error()
def get_sql_table_columns(table_name_with_schema, connection:Connection):
    """
    Get column names for a give table from information schema
    """
    sql_str = f'''
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE LOWER(CONCAT_WS('.', table_schema, table_name)) = LOWER('{remove_square_parenthesis(table_name_with_schema)}')
    ORDER BY ORDINAL_POSITION
    '''

    columns = []
    res = execute_sql_queries([sql_str], connection=connection)
    if res[0]:
        columns = [c[0] for c in res[0]]

    return columns



# %%

@catch_error()
def add_sql_new_columns_if_any(table_name_with_schema:str, columns:List, connection:Connection):
    """
    Add new columns to existing table, if any.
    """
    existing_columns = get_sql_table_columns(table_name_with_schema=table_name_with_schema, connection=connection)
    if not existing_columns: return ['No existing_columns']
    existing_columns = [c.lower() for c in existing_columns]

    new_columns = [c for c in columns if c.lower() not in existing_columns]
    if not new_columns: return ['No new_columns']

    new_col_str = ','.join([f'{c} {default_data_type}' for c in new_columns])
    add_new_columns_sql = f'ALTER TABLE {table_name_with_schema} ADD {new_col_str}'

    outputs = execute_sql_queries(sql_list=[add_new_columns_sql], connection=connection)
    return outputs



# %%

@catch_error()
def create_or_truncate_sql_table(table_name_with_schema:str, columns:List, connection:Connection, truncate:bool=False):
    """
    Create a new table if not exists otherwise optionally truncate. Add new columns to existing table, if any.
    """
    sql_list = []

    columns_sql = ',\n'.join([f'[{column}] {default_data_type}' for column in columns])
    create_table_sql = f'''
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(CONCAT_WS('.', table_schema, table_name)) = LOWER('{remove_square_parenthesis(table_name_with_schema)}'))
    BEGIN
        CREATE TABLE {table_name_with_schema} (
            {columns_sql}
        );
    END
    '''
    sql_list.append(create_table_sql)

    if truncate:
        truncate_table_sql = f'TRUNCATE TABLE {table_name_with_schema}'
        sql_list.append(truncate_table_sql)

    outputs = execute_sql_queries(sql_list=sql_list, connection=connection)

    outputs1 = add_sql_new_columns_if_any(table_name_with_schema=table_name_with_schema, columns=columns, connection=connection)
    outputs.extend(outputs1)

    return outputs



# %%


