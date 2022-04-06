description = """

Create Paths for data source at the local server

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    #parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        #'pipelinekey': 'MAINTENANCE_CREATE_SOURCE_PATHS',
        }



# %% Import Libraries

import os, sys, pymssql

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, logger, mark_execution_end, pipeline_metadata_conf



# %% Parameters



# %% Create folders for data sources at the local server

@catch_error(logger)
def create_folders():
    """
    Create folders for data sources at the local server
    """

    schema_name = pipeline_metadata_conf['sql_schema']
    table_name = pipeline_metadata_conf['sql_table_name_pipe_config']
    full_table_name = f'{schema_name}.{table_name}'

    sqlstr = f"""SELECT DISTINCT ConfigValue FROM {full_table_name} WHERE UPPER(ConfigKey) LIKE 'SOURCE_PATH%' ORDER BY ConfigValue;"""

    with pymssql.connect(
        server = pipeline_metadata_conf['sql_server'],
        user = pipeline_metadata_conf['sql_id'],
        password = pipeline_metadata_conf['sql_pass'],
        database = pipeline_metadata_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr)
            for row in cursor:
                folder_path = row['ConfigValue'].strip()
                logger.info(f'Creating folder: {folder_path}')
                try:
                    os.makedirs(folder_path, exist_ok=True)
                except Exception as e:
                    logger.warning(f'Error in creating folder at {folder_path} -> {str(e)}')

    logger.info('Finished creating all folders')



create_folders()



# %% Close Connections / End Program

mark_execution_end()



# %%


