"""
Module to handle common sql server migration tasks

"""


# %%

import pyodbc, os, csv, re
from collections import OrderedDict
from typing import List, Dict
from datetime import datetime

from .settings import Config
from .logger import logger, catch_error, environment
from .connections import Connection
from .common import run_process, remove_square_parenthesis
from .filemeta import FileMeta



# %% Parameters



# %%

@catch_error()
def bcp_to_sql_server_csv(file_path:str, connection:Connection, table_name_with_schema:str, delimiter:str=','):
    """
    Send CSV/PSV data file to SQL Server using bcp tool
    if no username or password given, then use trusted connection

    bcp <databse_name>.<schema_name>.<table_name> in "<file_path>" -S <server_name>.<dns_suffix> -U <username> -P <password> -c -t "|" -F 2

    bcp SaviyntIntegration.dbo.envestnet_hierarchy_firm in "C:/myworkdir/data/envestnet_v35_processed/hierarchy_firm_20231009.txt" -S DW1SQLDATA01.ibddomain.net -T -c -t "|" -F 2
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

    if environment.environment < environment.qa:
        logger.info(f'BCP Command: {bcp_str}')

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

@catch_error()
def convert_csv_to_psv(file_path:str, config:Config):
    """
    Convert CSV to PSV for corrent BCP tool function
    """
    file_name = os.path.basename(file_path)
    output_file = os.path.join(config.temporary_folder_path, file_name)
    logger.info(f'Converting CSV to PSV -> CSV: {file_path} PSV: {output_file}')

    first_flag = True
    with open(file=output_file, mode='w', newline='', encoding='UTF-8') as out_file:
        with open(file=file_path, mode='rt', newline='', encoding='UTF-8-SIG', errors='ignore') as csvfile:
            reader = csv.DictReader(f=csvfile, delimiter=',')
            for row in reader:
                rowl = OrderedDict()
                for k, v in row.items():
                    val = re.sub(r'\s', ' ', re.sub(r'\|', ':', v), flags=re.MULTILINE)
                    rowl[k] = val

                if first_flag:
                    first_flag = False
                    out_writer = csv.DictWriter(out_file, delimiter='|', quotechar=None, quoting=csv.QUOTE_NONE, skipinitialspace=True, fieldnames=row.keys())
                    out_writer.writeheader()
                out_writer.writerow(rowl)

    return output_file



# %%

@catch_error()
def bcp_to_sql_server(file_meta:FileMeta, connection:Connection, staging_table:str, config:Config):
    """
    Main BCP function, which calls other BCP functions depending on file_type
    """
    if file_meta.file_type == 'csv':
        file_path = file_meta.file_path
        delimiter = file_meta.delimiter
        if delimiter == ',':
            file_path = convert_csv_to_psv(file_path=file_path, config=config)
            delimiter = '|'

        file_meta.rows_copied = bcp_to_sql_server_csv(file_path=file_path, connection=connection, table_name_with_schema=staging_table, delimiter=delimiter)

    else:
        raise ValueError(f'Unknown file_meta.file_type: {file_meta.file_type}')

    return file_meta.rows_copied



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
            if environment.environment < environment.qa:
                logger.info(sql_str)
            cursor.execute(sql_str)
            if cursor.description:
                results = cursor.fetchall()
            elif cursor.rowcount>=0:
                results = [f'{cursor.rowcount} row(s) affected']
            else:
                results = ['Statement executed successfully']
            outputs.append(results)
        conn.commit()
    return outputs



# %%

@catch_error()
def sql_table_exists(table_name_with_schema, connection:Connection):
    """
    Check if SQL Server table exists
    """
    exists_sql = f'''
        SELECT COUNT(*) AS CNT
        FROM INFORMATION_SCHEMA.TABLES
        WHERE LOWER(CONCAT_WS('.', table_schema, table_name)) = LOWER('{remove_square_parenthesis(table_name_with_schema)}')
        '''

    output = execute_sql_queries(sql_list=[exists_sql], connection=connection)
    return output[0][0][0]>0



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
def add_sql_new_columns_if_any(table_name_with_schema:str, columns:OrderedDict, connection:Connection):
    """
    Add new columns to existing table, if any.
    """
    existing_columns = get_sql_table_columns(table_name_with_schema=table_name_with_schema, connection=connection)
    if not existing_columns: return ['No existing_columns']
    existing_columns = [c.lower() for c in existing_columns]

    new_columns = {c:t for c, t in columns.items() if c.lower() not in existing_columns}
    if not new_columns: return ['No new_columns']

    new_col_str = ','.join([f'{c} {t}' for c, t in new_columns.items()])
    add_new_columns_sql = f'ALTER TABLE {table_name_with_schema} ADD {new_col_str}'

    outputs = execute_sql_queries(sql_list=[add_new_columns_sql], connection=connection)
    return outputs



# %%

@catch_error()
def create_or_truncate_sql_table(table_name_with_schema:str, columns:OrderedDict, connection:Connection, truncate:bool=False):
    """
    Create a new table if not exists otherwise optionally truncate. Add new columns to existing table, if any.
    """
    sql_list = []

    columns_sql = ',\n'.join([f'[{c}] {t}' for c, t in columns.items()])
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

@catch_error()
def drop_sql_table_if_exists(table_name_with_schema:str, connection:Connection):
    """
    Drop table if exists
    """
    drop_table_sql = f'''
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(CONCAT_WS('.', table_schema, table_name)) = LOWER('{remove_square_parenthesis(table_name_with_schema)}'))
    BEGIN
        DROP TABLE {table_name_with_schema};
    END
    '''

    outputs = execute_sql_queries(sql_list=[drop_table_sql], connection=connection)
    return outputs



# %%

@catch_error()
def delete_history_from_sql_table(table_name_with_schema:str, connection:Connection):
    """
    Hard delete soft-deleted records
    """
    delete_history_sql = f'DELETE FROM {table_name_with_schema} WHERE meta_is_current=0;'
    outputs = execute_sql_queries(sql_list=[delete_history_sql], connection=connection)
    return outputs



# %%

@catch_error()
def get_sql_table_meta_columns(file_meta:FileMeta, config:Config):
    """
    Get Meta fileds that will be added to the table
    Output is a dictionary of:
        Key: Name of Column
        Value: tuple(Value/Expression, Column Type)
    """
    columns_sql = ','.join([f"COALESCE({c}, '')" for c in file_meta.columns])
    row_hash_sql = f"HASHBYTES('SHA2_256', CONCAT({columns_sql}))"

    meta_fields = OrderedDict([
        (f'{config.column_meta_prefix}pipeline_key', (config.default_data_type, f"'{file_meta.pipeline_key}'")),
        (f'{config.column_meta_prefix}is_full_load', ('INT', f"'{int(file_meta.is_full_load)}'")),
        (f'{config.column_meta_prefix}run_date', ('DATETIME', f"'{file_meta.run_date}'")),
        (f'{config.column_meta_prefix}date_of_data', ('DATETIME', f"'{file_meta.date_of_data}'")),
        (f'{config.column_meta_prefix}start_date', ('DATETIME', 'CURRENT_TIMESTAMP')),
        (f'{config.column_meta_prefix}end_date', ('DATETIME', "CAST('9999-12-31' AS DATETIME)")),
        (f'{config.column_meta_prefix}is_current', ('INT', '1')),
        (f'{config.column_meta_prefix}hash_key', ('VARBINARY(MAX)', row_hash_sql)),
    ])

    if file_meta.additional_info:
        for c, v in file_meta.additional_info.items():
            meta_fields[f'{config.column_meta_prefix}{c}'] = (config.default_data_type, v)

    meta_columns, meta_columns_values = OrderedDict(), OrderedDict()
    for c, v in meta_fields.items():
        meta_columns[c], meta_columns_values[c] = v

    return meta_columns, meta_columns_values



# %%

@catch_error()
def update_sql_staging_table_meta_fields(table_name_with_schema:str, meta_columns_values:OrderedDict, connection:Connection):
    """
    Update sql staging table meta fields
    """
    update_columns = ','.join([f'{c}={v}' for c, v in meta_columns_values.items()])
    update_table_sql = f'UPDATE {table_name_with_schema} SET {update_columns}'
    outputs = execute_sql_queries(sql_list=[update_table_sql], connection=connection)
    return outputs



# %%

@catch_error()
def merge_sql_staging_into_target(tgt_table_name_with_schema:str, stg_table_name_with_schema:str, all_columns:OrderedDict, is_full_load:bool, connection:Connection):
    """
    Merge-Match statement for SCD Type 2 table
    Assumes data is ingested in chronological order (i.e. we don't ingest old data after ingesting new data)
    Assumes no two processes try to update the same table at the same time
    """
    not_matched_by_source_sql = '''
    WHEN NOT MATCHED BY SOURCE AND TGT.META_IS_CURRENT = 1 THEN
        UPDATE SET
            TGT.META_IS_CURRENT = 0,
            TGT.META_END_DATE = CURRENT_TIMESTAMP
    '''
    if not is_full_load:
        not_matched_by_source_sql = ''

    merge_match_sql = f'''
    MERGE INTO {tgt_table_name_with_schema} TGT
    USING {stg_table_name_with_schema} SRC
        ON COALESCE(TGT.META_HASH_KEY, '') = COALESCE(SRC.META_HASH_KEY, '')
            AND TGT.META_IS_CURRENT = 1
    WHEN MATCHED THEN
        UPDATE SET
            TGT.META_PIPELINE_KEY = SRC.META_PIPELINE_KEY,
            TGT.META_RUN_DATE = SRC.META_RUN_DATE,
            TGT.META_DATE_OF_DATA = SRC.META_DATE_OF_DATA,
            TGT.META_IS_FULL_LOAD = SRC.META_IS_FULL_LOAD
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({', '.join(all_columns)})
        VALUES ({', '.join(['SRC.'+c for c in all_columns])})
    {not_matched_by_source_sql}
    ;
    '''

    outputs = execute_sql_queries(sql_list=[merge_match_sql], connection=connection)
    return outputs



# %%

@catch_error()
def migrate_file_to_sql_table(file_meta:FileMeta, connection:Connection, config:Config):
    """
    end-to-end from file_meta to final sql table with SCD Type 2 applied
    """
    if not file_meta:
        logger.warning(f'No File Meta, skipping')
        return

    migration_start_time = datetime.now()

    staging_table =  config.staging_schema + '.' + file_meta.table_name_with_schema.split('.')[1]
    logger.info(f'Preparing Staging Table: {staging_table}')

    drop_sql_table_if_exists(table_name_with_schema=staging_table, connection=connection)
    create_or_truncate_sql_table(table_name_with_schema=staging_table, columns=file_meta.columns, connection=connection, truncate=True)
    bcp_output = bcp_to_sql_server(file_meta=file_meta, connection=connection, staging_table=staging_table, config=config)

    if bcp_output is None or file_meta.rows_copied == 0:
        logger.warning(f'No rows copied in file, skipping {file_meta.file_path}')
    else:
        meta_columns, meta_columns_values = get_sql_table_meta_columns(file_meta=file_meta, config=config)
        add_sql_new_columns_if_any(table_name_with_schema=staging_table, columns=meta_columns, connection=connection)
        update_sql_staging_table_meta_fields(table_name_with_schema=staging_table, meta_columns_values=meta_columns_values, connection=connection)

        logger.info(f'Preparing Persistent Table: {file_meta.table_name_with_schema}')

        recreate_target_table = False
        if hasattr(config, 'recreate_target_table') and config.recreate_target_table.strip().upper() == 'TRUE':
            recreate_target_table = True
        if recreate_target_table:
            drop_sql_table_if_exists(table_name_with_schema=file_meta.table_name_with_schema, connection=connection)

        create_or_truncate_sql_table(table_name_with_schema=file_meta.table_name_with_schema, columns=file_meta.columns | meta_columns, connection=connection, truncate=False)

        output = merge_sql_staging_into_target(
            tgt_table_name_with_schema = file_meta.table_name_with_schema,
            stg_table_name_with_schema = staging_table,
            all_columns = file_meta.columns | meta_columns,
            is_full_load = file_meta.is_full_load,
            connection = connection,
            )

        keep_history = True
        if hasattr(config, 'keep_history') and config.keep_history.strip().upper() == 'FALSE':
            keep_history = False
        if not keep_history:
            delete_history_from_sql_table(table_name_with_schema=file_meta.table_name_with_schema, connection=connection)

        logger.info(f'Finished Merge-Match statement for table {file_meta.table_name_with_schema}')

    drop_sql_table_if_exists(table_name_with_schema=staging_table, connection=connection)

    timedelta1 = datetime.now() - migration_start_time
    file_meta.load_time_seconds = timedelta1.days*86400 + timedelta1.seconds + timedelta1.microseconds/10**6



# %%


