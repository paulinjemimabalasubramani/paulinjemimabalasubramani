description = """

Generic Code to Copy Tables from SQL Server to local store as csv files

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
        'pipelinekey': 'FP_MIGRATE_LR',
        'source_path': r'C:\myworkdir\Shared\LR',
        'sql_table_list': r'C:\myworkdir\EDIP-Code\config\lookup_files\LNR_Tables.csv',
        }



# %% Import Libraries

import os, sys, re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc, get_csv_rows, get_secrets, column_regex
from modules3.spark_functions import create_spark, read_sql



# %% Parameters

csv_file_ext = '.txt'



# %% Create Connections

spark = create_spark()



# %% Get Table List

@catch_error(logger)
def get_table_list():
    """
    Get list of tables to ingest
    """
    table_list = []

    for row in get_csv_rows(csv_file_path=data_settings.sql_table_list):
        if row['table_of_interest'].upper() in ['Y', 'YES', '1']:
            table_list.append((row['schemaname'], row['tablename']))

    return table_list



table_list = get_table_list()



# %% Copy all SQL Tables and save as CSV files

@catch_error(logger)
def copy_all_sql_tables():
    """
    Copy all SQL Tables and save as CSV files
    """
    if not table_list:
        logger.warning('No tables found in table_list')
        return

    logger.info(f'Copying {len(table_list)} tables in table_list in SQL server: {data_settings.sql_server}, SQL database: {data_settings.sql_database}')
    _, sql_user, sql_pass = get_secrets(data_settings.sql_key_vault_account.lower(), logger=logger)

    error_tables = []
    for schema_name, table_name in table_list:
        logger.info(f'Reading table {schema_name}.{table_name}')

        try:
            table = read_sql(
                spark = spark,
                schema = schema_name,
                table_name = table_name,
                database = data_settings.sql_database,
                server = data_settings.sql_server,
                user = sql_user,
                password = sql_pass,
                )
        except Exception as e:
            logger.warning(f'Error in reading table {schema_name}.{table_name}.')
            error_tables.append(f'{schema_name}.{table_name}')
            continue

        if not table:
            logger.warning(f'Table {schema_name}.{table_name} is empty - not copying')
            error_tables.append(f'{schema_name}.{table_name}')
            continue

        for c in table.columns:
            table = table.withColumnRenamed(c, re.sub(column_regex, '_', c.strip().lower()))

        csv_file_name = f'{schema_name}_{table_name}{csv_file_ext}'.lower()
        csv_path = os.path.join(data_settings.source_path, csv_file_name)
        logger.info(f'Writing table {schema_name}.{table_name} to {csv_path}')
        table.coalesce(1).write.csv(path=csv_path, header='true', mode='overwrite')
        logger.info(f'Finished writing table {schema_name}.{table_name} to {csv_path}')

    logger.info(f'Finished Copying {len(table_list)} tables')
    if error_tables:
        logger.warning(f'Following {len(error_tables)} tables had error or was empty: {error_tables}')
    else:
        logger.info(f'No errors found file copying tables')



copy_all_sql_tables()



# %% Close Connections / End Program

mark_execution_end()


# %%


