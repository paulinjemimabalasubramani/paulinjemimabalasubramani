description = """

Generic Code to download tables from SQL Server

sp_spaceused '[dbo].[D_Rep]';
sp_spaceused '[AUM].[DWRep]';

"""


# %% Parse Arguments

if False: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'COMM_MIGRATE_CLIENT_REVENUE',
        'source_path': r'C:\EDIP_Files\sql_server_download',
        'trusted_connection': 'TRUE',
        'sql_server': 'DW1SQLDATA01.ibddomain.net',
        'sql_database': 'DW',
        }



# %% Import Libraries

import os, sys, pyodbc

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, get_secrets, normalize_name, run_process



# %% Paramters

download_file_ext = '.txt'
delimiter = '#\!#\!'

# %%

os.makedirs(data_settings.source_path, exist_ok=True)


# %%

#### ADD COLUMN NAMES TO FILE HEADER!!!
# bcp "select 'SalesOrderID', 'CarrierTrackingNumber','ModifiedDate','UpdateTime' union all SELECT convert(varchar(20),[SalesOrderID]),convert(varchar(20),[CarrierTrackingNumber]),CONVERT(nvarchar(30), [ModifiedDate], 120),CONVERT(nvarchar(30), [UpdateTime], 120) FROM [testdb].[dbo].[SalesOrderDetailIn]" queryout D:\People.txt -t, -c -T

table_name_with_schema = '[AUM].[DWRep]'
sql_query = f'select * from {table_name_with_schema}(nolock)'

file_name = normalize_name(table_name_with_schema) + download_file_ext
file_path = os.path.join(data_settings.source_path, file_name)


@catch_error()
def download_sql_server(file_path:str, sql_query:str, delimiter:str=delimiter):
    """
    Download data from SQL Server using bcp tool and save to a file

    bcp "select * from [edip].[agfirm](nolock)" queryout "./agfirm.txt" -S DW1SQLOLTP02.ibddomain.net -d FinancialProfessional -U svc_fpapp -P svc_fpapp -c -t#\!#\! -C RAW

    https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16
    """
    if getattr(data_settings, 'trusted_connection', 'FALSE') == 'TRUE':
        authentication_str = '-T'
    else:
        _, sql_user, sql_pass = get_secrets(data_settings.sql_key_vault_account.lower(), logger=logger)
        authentication_str = f"-U {sql_user} -P '{sql_pass}'"

    bcp_str_ex_auth = f'bcp "{sql_query}" queryout "{file_path}" -S {data_settings.sql_server} -d {data_settings.sql_database} -c -t "{delimiter}" -C RAW'
    bcp_str = f'{bcp_str_ex_auth} {authentication_str}'

    if is_pc:
        logger.info(f'BCP Command: {bcp_str_ex_auth}')

    stdout = None
    try:
        stdout = run_process(command=bcp_str, mask_error=True, hint=bcp_str_ex_auth)
    except Exception as e:
        logger.error(f'Error in running BCP command: {bcp_str_ex_auth}')
    if not stdout: return

    try:
        rows_copied = int(stdout[:stdout.find(' rows copied.')].split('\n')[-1])
    except Exception as e:
        logger.error(f'Exceptin on extracting rows_copied: {str(e)}')
        return

    logger.info(f'Server: {data_settings.sql_server}; Table: {data_settings.sql_database}.{table_name_with_schema}; Rows copied: {rows_copied}; File: {file_path}')
    return rows_copied


download_sql_server(file_path=file_path, sql_query=sql_query)



# %%









