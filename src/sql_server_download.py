description = """
Generic Code to download tables from SQL Server

CREATE TABLE [metadata].[database_server_ingestion] (
  [id] int IDENTITY(1,1) NOT NULL PRIMARY KEY,
  [pipeline_key] varchar(255) NOT NULL,
  [database_platform] varchar(255) NOT NULL,
  [key_vault] varchar(255) NOT NULL,
  [server_name] varchar(255) NOT NULL,
  [database_name] varchar(255) NOT NULL,
  [table_name_with_schema] varchar(255) NOT NULL,
  [custom_query] nvarchar(2000) NULL,
  [custom_columns] nvarchar(2000) NULL,
  [comments] nvarchar(2000) NULL,
  [created_at] DATETIME DEFAULT (CURRENT_TIMESTAMP)
);

INSERT INTO [metadata].[database_server_ingestion] ([pipeline_key],[database_platform],[key_vault],[server_name],[database_name],[table_name_with_schema],[custom_query],[custom_columns],[comments]
)
VALUES (
  'COMM_MIGRATE_CLIENT_REVENUE', -- Replace with your actual pipeline key
  'MSSQL',  -- Replace with the database platform (e.g., MySQL, PostgreSQL)
  'MyVaultName',  -- Replace with the key vault name (if applicable)
  'DW1SQLDATA01.ibddomain.net', -- Replace with the server name or IP address
  'DW', -- Replace with the database name
  '[AUM].[DWRep]', -- Replace with the table name with schema (if applicable)
  NULL, -- Custom query (replace with your actual query if needed)
  NULL, -- Custom columns (replace with comma-separated list if needed)
  NULL -- Replace with your comments
);


sp_spaceused '[dbo].[D_Rep]';
sp_spaceused '[AUM].[DWRep]';


SELECT STRING_AGG(COLUMN_NAME, ',') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS COLUMNS
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = 'SupervisionControls'
    AND TABLE_SCHEMA = 'Affirm'
    AND TABLE_NAME = 'ArrDestination'
;

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'REPLICA_MIGRATE_PW1SQLREPT01_SUPERVISIONCONTROLS',
        'source_path': r'C:\EDIP_Files\sql_server_download',
        'sql_driver': '{ODBC Driver 17 for SQL Server}',
        }



# %% Import Libraries

import os, sys, pyodbc, shutil,csv

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from typing import Union, List, Dict
from airflow.models import XCom

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, get_secrets, normalize_name, run_process,\
    remove_square_parenthesis, pipeline_metadata_conf, execute_sql_queries, KeyVaultList, Connection,process_file

from airflow.models import Variable


# %% Paramters

download_file_ext = '.txt'
header_file_ext = '_header.txt'
body_file_ext = '_body.txt'

delimiter = '#\!#\!'
delimiter = data_settings.get_value('bcp_delimiter','|')
carriage_return = '$#$#'
carriage_return = data_settings.get_value('bcp_carriage_return',None)

driver_map = {
    'MSSQL': '{ODBC Driver 17 for SQL Server}',
}



# %%

key_vault = KeyVaultList()



# %%

os.makedirs(data_settings.source_path, exist_ok=True)



# %%

@catch_error(logger)
def get_sql_table_columns(table_name_with_schema:str, connection:Connection) -> List:
    """
    Get column names for a give table from information schema
    """
    sql_str = f'''
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
    WHERE LOWER(CONCAT_WS('.', table_schema, table_name)) = LOWER('{remove_square_parenthesis(table_name_with_schema)}')
    ORDER BY ORDINAL_POSITION
    '''

    columns = []
    res = execute_sql_queries([sql_str], connection_string=connection.get_connection_str_sql())
    if res[0]:
        columns = [c[0] for c in res[0]]

    return columns



# %%

@catch_error(logger)
def download_sql_server_query_to_file(file_path:str, sql_query:str, connection:Connection, delimiter:str=delimiter, carriage_return:str=carriage_return, bcp_batch_size:int=20000, bcp_packet_size:int=32768) -> int:
    """
    Download data from SQL Server using bcp tool and save to a file

    bcp "select * from [edip].[agfirm](nolock)" queryout "./agfirm.txt" -S DW1SQLOLTP02.ibddomain.net -d FinancialProfessional -U svc_fpapp -P svc_fpapp -c -t#\!#\! -C RAW

    https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16
    """
    bcp_str_ex_conn = f'bcp "{sql_query}" queryout "{file_path}" -c -t "{delimiter}" -C RAW -b {bcp_batch_size} -a {bcp_packet_size}'
    if carriage_return:
        bcp_str_ex_conn += f' -r {carriage_return}'
    bcp_str = f'{bcp_str_ex_conn} {connection.get_connection_str_bcp()}'
    logger.info(bcp_str_ex_conn)

    if is_pc:
        print(f'BCP Command: {bcp_str}')

    stdout = None
    try:
        stdout = run_process(command=bcp_str, mask_error=True, hint=bcp_str_ex_conn)
        if delimiter != '|' and carriage_return is not None:
            process_file(source_file=file_path,record_separator=carriage_return)

    except Exception as e:
        logger.error(f'Error in running BCP command: {bcp_str_ex_conn}')
    if not stdout: return

    try:
        rows_copied = int(stdout[:stdout.find(' rows copied.')].split('\n')[-1])
    except Exception as e:
        logger.error(f'Exception on extracting rows_copied: {str(e)}')
        return

    logger.info(f'Rows copied: {rows_copied}; File: {file_path}; Query: {sql_query}')
    return rows_copied



# %% NOT USED !!!!!!!

@catch_error(logger)
def get_sql_query_from_table_tuple_not_used(table_info:Dict, connection:Connection):
    """
    Construct SQL query from Table Name / Custom Table Tuple (table_name_with_schema, custom_query, custom_columns)

    # Alternate Option:
    columns_list_str = "'" + "', '".join(columns) + "'"
    columns_convert_nvarchar_str = ", ".join([f"NULLIF(REPLACE(CONVERT(NVARCHAR(2000),[{c}]),'|',''),'')" for c in columns])

    sql_query = f'''
    SELECT {columns_list_str}
    UNION ALL
    SELECT {columns_convert_nvarchar_str}
    FROM {table_name_with_schema}(NOLOCK)
    '''
    """
    table_name_with_schema = table_info['table_name_with_schema']
    sql_query = table_info['custom_query'].strip() if table_info['custom_query'] else ''

    columns = None
    if not sql_query:
        columns = get_sql_table_columns(table_name_with_schema=table_name_with_schema, connection=connection)
        sql_query = f"SELECT {','.join(columns)} FROM {table_name_with_schema}(NOLOCK)"

    columns_raw = table_info['custom_columns'].strip() if table_info['custom_columns'] else ''
    if columns_raw:
        columns = [c.strip() for c in columns_raw.lower().split(',') if c.strip()]
        sql_query = f"SELECT {columns_raw} FROM ({sql_query}) T"

    if not columns:
        logger.error(f'No columns data for {table_name_with_schema}, skippping')
        return None, None, None

    return table_name_with_schema, sql_query, columns

@catch_error(logger)
def check_get_one_time_history_data_from_table(custom_query:str,table_name_with_schema:str, ti=None):
    if data_settings.get_value('one_time_history',None) and data_settings.get_value('one_time_history_csv_config_path',None):
        csv_file = data_settings.get_value('one_time_history_csv_config_path',None)
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            process_date = next((row for row in reader if row['STATUS'] == 'NOT_YET_LOADED'), None)
        
        if process_date:            
            custom_query = custom_query.format(START_DATE=process_date['START_DATE'],END_DATE=process_date['END_DATE'])
            logger.info(f"History data load query => table_name_with_schema : {table_name_with_schema},Process Start date : {process_date['START_DATE']},Process End date :{process_date['END_DATE']},custom_query : {custom_query} ")
            
            ti.xcom_push(key='history_year_quarter', value=process_date['YEAR_QUARTER']) 
            ti.xcom_push(key='start_date', value=process_date['START_DATE']) 
            ti.xcom_push(key='end_date', value=process_date['END_DATE'])
            
        else:
            logger.info(f'All history load is completed for table: {table_name_with_schema} Query: {custom_query}')
            custom_query = None        
            
    return custom_query

# %%

@catch_error(logger)
def get_sql_query_from_table_tuple(table_info:Dict, connection:Connection):
    """
    Construct SQL query from Table Name / Custom Table Tuple (table_name_with_schema, custom_query, custom_columns)
    """
    table_name_with_schema = table_info['table_name_with_schema']
    custom_query = table_info['custom_query'].strip() if table_info['custom_query'] else ''
    custom_columns = table_info['custom_columns'].strip() if table_info['custom_columns'] else ''

    custom_query = check_get_one_time_history_data_from_table(custom_query=custom_query,table_name_with_schema=table_name_with_schema)

    if not custom_query and data_settings.get_value('one_time_history',None):
        return None, None, None

    if custom_query and not custom_columns and (delimiter != '|' or carriage_return is not None):
        logger.error(f'Custom query is given without any custom columns. Table: {table_name_with_schema} Query: {custom_query}')
        columns = get_sql_table_columns(table_name_with_schema=table_name_with_schema, connection=connection)
        columns_list_str = "'" + "', '".join(columns) + "'"
        sql_query = f'''
                    SELECT {columns_list_str}
                    UNION ALL
                     {custom_query}
                   ''' 
        return table_name_with_schema, sql_query, columns

    if not custom_query:
        custom_query = f'SELECT * FROM {table_name_with_schema}(NOLOCK)'

    if custom_columns:
        columns = [c.strip() for c in custom_columns.lower().split(',') if c.strip()]
    else:
        columns = get_sql_table_columns(table_name_with_schema=table_name_with_schema, connection=connection)

    columns_list_str = "'" + "', '".join(columns) + "'"
    columns_convert_nvarchar_str = ", ".join([f"NULLIF(REPLACE(REPLACE(REPLACE(CONVERT(NVARCHAR(MAX),{c}),'|',':'),CHAR(10),' '),CHAR(13),' '),'')" for c in columns])

    sql_query = f'''
    SELECT {columns_list_str}
    UNION ALL
    SELECT {columns_convert_nvarchar_str}
    FROM ({custom_query}) T
    '''

    return table_name_with_schema, sql_query, columns



# %% NOT USED !!!

@catch_error(logger)
def download_one_sql_table_not_used(table_info:Dict, connection=Connection):
    """
    Download one sql table to a file
    """
    table_name_with_schema, sql_query, columns  = get_sql_query_from_table_tuple(table_info=table_info, connection=connection)
    logger.info(f'Downloading table: {table_name_with_schema}')

    body_file_name = normalize_name(table_name_with_schema) + body_file_ext
    body_file_path = os.path.join(data_settings.source_path, body_file_name)

    download_file_name = normalize_name(table_name_with_schema) + download_file_ext
    download_file_path = os.path.join(data_settings.source_path, download_file_name)

    rows_copied = download_sql_server_query_to_file(file_path=body_file_path, sql_query=sql_query, connection=connection)

    if rows_copied and rows_copied>0:
        with open(file=download_file_path, mode='wt', encoding='UTF-8') as download_file:
            carr_return = carriage_return if carriage_return else '\n'
            download_file.write(delimiter.replace('\\','').join(columns) + carr_return)

        logger.info(f'Merging files {download_file_path} + {body_file_path}')
        with open(file=body_file_path, mode='rb') as f_in, open(file=download_file_path, mode='ab') as f_out:
            shutil.copyfileobj(f_in, f_out)
    else:
        logger.error(f'Error in downloading to a file {body_file_path} using SQL query: {sql_query}')

    logger.info(f'Removing body file {body_file_path}')
    os.remove(path=body_file_path)
    return rows_copied



# %%

@catch_error(logger)
def download_one_sql_table(table_info:Dict, connection=Connection):
    """
    Download one sql table to a file
    """
    table_name_with_schema, sql_query, columns  = get_sql_query_from_table_tuple(table_info=table_info, connection=connection)
    logger.info(f'Downloading table: {table_name_with_schema}')

    download_file_name = normalize_name(table_name_with_schema) + download_file_ext
    download_file_path = os.path.join(data_settings.source_path, download_file_name)

    rows_copied = download_sql_server_query_to_file(file_path=download_file_path, sql_query=sql_query, connection=connection)
    return rows_copied



# %%

@catch_error(logger)
def get_table_info_list():
    """
    Get list of tables to ingest
    """
    master_list_name_table = '[database_server_ingestion]'

    logger.info(f"Getting list of tables from SQL Server {pipeline_metadata_conf['sql_server']} Database {pipeline_metadata_conf['sql_database']} Table {pipeline_metadata_conf['sql_schema']}.{master_list_name_table}")

    connection = Connection(
        driver = data_settings.sql_driver,
        server = pipeline_metadata_conf['sql_server'],
        database = pipeline_metadata_conf['sql_database'],
        username = pipeline_metadata_conf['sql_id'],
        password = pipeline_metadata_conf['sql_pass'],
    )

    database_server_ingestion_columns = [
        'database_platform',
        'key_vault',
        'server_name',
        'database_name',
        'table_name_with_schema',
        'custom_query',
        'custom_columns',
    ]

    sql_str = f"""
    SELECT {','.join(database_server_ingestion_columns)}
    FROM {pipeline_metadata_conf['sql_schema']}.{master_list_name_table}
    WHERE pipeline_key = '{data_settings.pipelinekey.upper()}'
    """

    table_info_list = []
    table_info_list_raw = execute_sql_queries(sql_list=sql_str, connection_string=connection.get_connection_str_sql())
    for table_info_raw in table_info_list_raw[0]:
        table_info_list.append({c:table_info_raw[i] for i, c in enumerate(database_server_ingestion_columns)})

    return table_info_list



# %%

@catch_error(logger)
def download_all_sql_tables():
    """
    Main function to download all tables
    """
    table_info_list = get_table_info_list()

    for table_info in table_info_list:
        connection = Connection(
            driver = driver_map[table_info['database_platform']],
            server = table_info['server_name'],
            database = table_info['database_name'],
            key_vault_name = table_info['key_vault'],
            trusted_connection = is_pc,
            key_vault = key_vault,
        )

        download_one_sql_table(table_info=table_info, connection=connection)



download_all_sql_tables()



# %% Close Connections / End Program

mark_execution_end()



# %%


