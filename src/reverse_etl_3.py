description = """
Move curated data from Snowflake back to on prem SQL Server

https://docs.snowflake.com/en/user-guide/spark-connector.html
https://docs.databricks.com/_static/notebooks/snowflake-python.html

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
        'pipelinekey': 'REVERSE_ETL_FP_EDIP',
        'data_type_translation_file': r'C:\myworkdir\EDIP-Code\config\lookup_files\DataTypeTranslation.csv',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import logger, catch_error, get_secrets, mark_execution_end, data_settings, pymssql_execute_non_query
from modules3.spark_functions import create_spark, write_sql, read_snowflake
from modules3.migrate_files import get_data_type_translation
from modules3.snowflake_ddl import snowflake_ddl_params, connect_to_snowflake, fetchall_snowflake



# %% Parameters

data_type_translation_id = 'snowflake_sqlserver'
translation = get_data_type_translation(data_type_translation_id=data_type_translation_id)

raw_schema_suffix = '_RAW'

hard_exclude_table_names = ['CICD_CHANGE_HISTORY']



# %% Create Connections

spark = create_spark()
snowflake_ddl_params.spark = spark
snowflake_ddl_params.snowflake_connection = connect_to_snowflake()



# %% Read Key Vault Data

_, sql_id, sql_pass = get_secrets(data_settings.sql_key_vault_account.lower(), logger=logger)
_, sf_id, sf_pass = get_secrets(data_settings.snowflake_key_vault_account.lower(), logger=logger)



# %% Get list of tables and views from Information Schema

@catch_error(logger)
def get_tables(tables_wildcard:str, table_type:str='BASE TABLE'):
    """
    Get list of tables or views from Information Schema
    """
    tables_wld_list = [f"UPPER(TABLE_NAME) LIKE '{x.strip().upper()}'" for x in tables_wildcard.split(',')]

    sqlstr = f"""
        SELECT TABLE_NAME
        FROM {snowflake_ddl_params.snowflake_database}.INFORMATION_SCHEMA.TABLES
        WHERE UPPER(TABLE_SCHEMA)='{data_settings.schema_name.upper()}'
            AND ({' OR '.join(tables_wld_list)})
            AND UPPER(TABLE_TYPE) = '{table_type.upper()}'
        ;
    """

    tables = fetchall_snowflake(sqlstr=sqlstr)
    tables = [x['TABLE_NAME'] for x in tables]
    return tables



tables = get_tables(tables_wildcard=data_settings.tables, table_type='BASE TABLE')
#views = get_tables(tables_wildcard=data_settings.views, table_type='VIEW')



# %% Get Columns for a given Table

@catch_error(logger)
def get_table_columns(table_name:str):
    """
    Get Columns for a given Table
    """
    sqlstr = f"""
        SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM {snowflake_ddl_params.snowflake_database}.INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_SCHEMA)='{data_settings.schema_name.upper()}'
            AND UPPER(TABLE_NAME) = '{table_name.upper()}'
        ;
    """

    columns = fetchall_snowflake(sqlstr=sqlstr)
    return columns



# %% Get Primary Keys

@catch_error(logger)
def get_table_primary_keys(table_name:str):
    """
    Get Primary Keys
    """
    sqlstr = f'SHOW PRIMARY KEYS IN TABLE {snowflake_ddl_params.snowflake_database}.{data_settings.schema_name}.{table_name};'

    primary_keys = fetchall_snowflake(sqlstr=sqlstr)
    primary_keys = [x['COLUMN_NAME'] for x in primary_keys]
    return primary_keys



# %% Re-create SQL Table with the latest schema

@catch_error(logger)
def recreate_raw_sql_table(table_name:str, columns:list, primary_keys:list, max_varchar_length:int=1000):
    """
    Re-create SQL Table with the latest schema
    """
    column_list = []
    for column in columns:
        target_data_type = translation.get(column['DATA_TYPE'].lower(), 'varchar').lower()
        col_sql = f"[{column['COLUMN_NAME']}] {target_data_type}"

        if target_data_type in ['varchar']:
            CHARACTER_MAXIMUM_LENGTH = column['CHARACTER_MAXIMUM_LENGTH'] if column['CHARACTER_MAXIMUM_LENGTH'] and column['CHARACTER_MAXIMUM_LENGTH']>0 and column['CHARACTER_MAXIMUM_LENGTH']<max_varchar_length else max_varchar_length
            if CHARACTER_MAXIMUM_LENGTH>0: col_sql += f'({CHARACTER_MAXIMUM_LENGTH})'
        elif target_data_type in ['decimal', 'numeric']:
            precision = column['NUMERIC_PRECISION'] if column['NUMERIC_PRECISION'] else 38
            scale = column['NUMERIC_SCALE'] if column['NUMERIC_SCALE'] else 0
            col_sql += f'({precision}, {scale})'

        col_sql += ' NULL' if column['IS_NULLABLE'].upper()=='YES' else ' NOT NULL'

        column_list.append((col_sql, column['ORDINAL_POSITION']))

    column_list = sorted(column_list, key=lambda x: x[1])
    column_list = [x[0] for x in column_list]
    column_list.append(f"CONSTRAINT PK_{table_name.upper()} PRIMARY KEY ({', '.join(primary_keys)})")
    column_list = '\n  ' + '\n ,'.join(column_list)

    sqlstr_drop = f'DROP TABLE IF EXISTS [{data_settings.sql_schema_name}{raw_schema_suffix}].[{table_name}];'
    sqlstr = f'CREATE TABLE [{data_settings.sql_schema_name}{raw_schema_suffix}].[{table_name}] ({column_list});'

    pymssql_execute_non_query(
        sqlstr_list = [sqlstr_drop, sqlstr],
        sql_server = data_settings.sql_server,
        sql_id = sql_id,
        sql_pass = sql_pass,
        sql_database = data_settings.sql_db_name,
    )



# %% Merge Raw SQL Table to final SQL Table

@catch_error(logger)
def merge_sql_table(table_name:str, columns:list, primary_keys:list):
    """
    Merge Raw SQL Table to final SQL Table
    """
    trim_coalesce = lambda x: f"LTRIM(RTRIM(COALESCE({x},'N/A')))"

    merge_on = '\n    AND '.join([f"{trim_coalesce('src.['+c+']')} = {trim_coalesce('tgt.['+c+']')}" for c in primary_keys])
    check_na = '\n    '.join([f"AND {trim_coalesce('src.['+c+']')} != 'N/A'" for c in primary_keys])
    update_set = '\n   ,'.join([f"tgt.[{c['COLUMN_NAME']}]=src.[{c['COLUMN_NAME']}]" for c in columns])
    insert_columns = '\n   ,'.join([f"[{c['COLUMN_NAME']}]" for c in columns])
    insert_values = '\n   ,'.join([f"src.[{c['COLUMN_NAME']}]" for c in columns])

    sqlstr = f"""MERGE INTO [{data_settings.sql_schema_name}].[{table_name}] tgt
USING [{data_settings.sql_schema_name}{raw_schema_suffix}].[{table_name}] src
ON {merge_on}
WHEN MATCHED {check_na}
THEN UPDATE SET {update_set}
WHEN NOT MATCHED {check_na}
THEN
  INSERT ({insert_columns})
  VALUES ({insert_values})
;
"""

    file_folder_path = os.path.join(data_settings.output_ddl_path, 'reverse_etl', data_settings.pipelinekey.upper())
    os.makedirs(name=file_folder_path, exist_ok=True)
    file_path = os.path.join(file_folder_path, f'{table_name}.sql')

    logger.info(f'Writing: {file_path}')
    with open(file_path, 'w') as f:
        f.write(sqlstr)

    pymssql_execute_non_query(
        sqlstr_list = [sqlstr],
        sql_server = data_settings.sql_server,
        sql_id = sql_id,
        sql_pass = sql_pass,
        sql_database = data_settings.sql_db_name,
    )



# %% Loop over all tables

@catch_error(logger)
def reverse_etl_all_tables():
    """
    Loop over all tables to read from Snowflake and write to SQL Server
    """
    _, snowflake_user, snowflake_pass = get_secrets(data_settings.snowflake_key_vault_account)

    for table_name in tables:
        if table_name in hard_exclude_table_names: continue

        columns = get_table_columns(table_name=table_name)
        primary_keys = get_table_primary_keys(table_name=table_name)
        if not primary_keys:
            logger.warning(f'No primary keys found for table {table_name}')
            continue

        try:
            table = read_snowflake(
                spark = spark,
                table_name = f'SELECT * FROM {table_name} WHERE SCD_IS_CURRENT_RECORD=1;',
                schema = data_settings.schema_name,
                database = snowflake_ddl_params.snowflake_database,
                warehouse = data_settings.snowflake_warehouse,
                role = data_settings.snowflake_role,
                account = data_settings.snowflake_account,
                user = snowflake_user,
                password = snowflake_pass,
                is_query = True,
                )

            recreate_raw_sql_table(
                table_name = table_name,
                columns = columns,
                primary_keys = primary_keys,
            )

            write_sql(
                table = table,
                table_name = table_name.lower(),
                schema = data_settings.sql_schema_name + raw_schema_suffix,
                database = data_settings.sql_db_name,
                server = data_settings.sql_server,
                user = sql_id,
                password = sql_pass,
                mode = 'append',
            )

            merge_sql_table(
                table_name = table_name,
                columns = columns,
                primary_keys = primary_keys,
                )

        except Exception as e:
            logger.error(str(e))

    logger.info(f'Finished Reverse ETL for all tables for {snowflake_ddl_params.snowflake_database}.{data_settings.schema_name}')



reverse_etl_all_tables()



# %% Mark Execution End

mark_execution_end()



# %%


