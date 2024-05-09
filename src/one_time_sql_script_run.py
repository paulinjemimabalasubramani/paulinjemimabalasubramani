description = """
One-time data manipulation in SQL Server

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
        'pipelinekey': 'COMM_MIGRATE_CLIENT_REVENUE',
        'source_path': r'C:\EDIP_Files\sql_server_download',
        'sql_driver': '{ODBC Driver 17 for SQL Server}',
        }



# %% Import Libraries

import os, sys, pyodbc, shutil

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from typing import Union, List, Dict

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, get_secrets, normalize_name, run_process,\
    remove_square_parenthesis, pipeline_metadata_conf, execute_sql_queries, KeyVaultList, Connection



# %%


connection = Connection(
    driver = data_settings.sql_driver,
    server = pipeline_metadata_conf['sql_server'],
    database = pipeline_metadata_conf['sql_database'],
    username = pipeline_metadata_conf['sql_id'],
    password = pipeline_metadata_conf['sql_pass'],
)


sql_str = f"""
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
"""


resutls = execute_sql_queries(sql_list=sql_str, connection_string=connection.get_connection_str_sql())


logger.info(resutls)


# %%

mark_execution_end()


