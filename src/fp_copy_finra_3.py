description = """

Copy FIRNA files from remote location to source location

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'FP_COPY_FINRA',

        'remote_path_fsc': r'',
        'remote_path_raa': r'C:\myworkdir\FINRA',
        'remote_path_saa': r'',
        'remote_path_sai': r'',
        'remote_path_spf': r'',
        'remote_path_trd': r'',
        'remote_path_wfs': r'',

        'source_path_fsc': r'C:\myworkdir\Shared\FINRA\COPY',
        'source_path_raa': r'C:\myworkdir\Shared\FINRA\COPY',
        'source_path_saa': r'C:\myworkdir\Shared\FINRA\COPY',
        'source_path_sai': r'C:\myworkdir\Shared\FINRA\COPY',
        'source_path_spf': r'C:\myworkdir\Shared\FINRA\COPY',
        'source_path_trd': r'C:\myworkdir\Shared\FINRA\COPY',
        'source_path_wfs': r'C:\myworkdir\Shared\FINRA\COPY',
        }



# %% Import Libraries

import os, sys, pymssql

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, relative_copy_file, collect_keys_from_config, cloud_file_hist_conf
from modules3.migrate_files import check_if_file_meta_table_exists
from modules3.finra_header import extract_data_from_finra_file_path



# %% Parameters



# %% Get Max Dates from SQL Server History

@catch_error(logger)
def get_max_key_datetimes():
    """
    Get Max Dates from SQL Server History per table
    """
    if not check_if_file_meta_table_exists():
        return {}

    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_hist_conf['sql_file_history_table']}".lower()

    sqlstr = f"SELECT table_name, MAX(key_datetime) AS maxdate FROM {full_table_name} GROUP BY table_name;"

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr)
            rows = cursor.fetchall()

    max_key_datetimes = {row['table_name'].lower():row['maxdate'] for row in rows}
    return max_key_datetimes



max_key_datetimes = get_max_key_datetimes()



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_file(firm:str, remote_path:str):
    """
    Copy files and folders to the new location
    """
    if not os.path.isdir(remote_path):
        logger.warning(f'Remote path does not exist: {remote_path}')
        return

    source_path_setting = 'source_path_' + firm.lower()
    if not hasattr(data_settings, source_path_setting):
        logger.warning(f'{source_path_setting} is not defined in SQL PipelineConfig for firm {firm}')
        return

    for file_name in os.listdir(remote_path):
        remote_file_path = os.path.join(remote_path, file_name)
        if not os.path.isfile(remote_file_path): continue

        file_meta = extract_data_from_finra_file_path(file_path=remote_file_path, get_firm_crd=False)
        table_name = firm.lower() + '_' + file_meta['table_name_no_firm']
        key_datetime = max(max_key_datetimes.get(table_name, data_settings.key_datetime), data_settings.key_datetime)
        if key_datetime > file_meta['file_date']: continue

        source_file_path = os.path.join(getattr(data_settings, source_path_setting), file_name)
        relative_copy_file(remote_path=remote_path, dest_path=source_file_path, remote_file_path=remote_file_path)



# %% Copy Files per Firm

@catch_error(logger)
def copy_files_per_firm():
    """
    Copy Files per firm
    """
    remote_paths = collect_keys_from_config(prefix='remote_path_', uppercase_key=True)

    for firm, remote_path in remote_paths.items():
        copy_file(firm=firm, remote_path=remote_path)



copy_files_per_firm()



# %% Close Connections / End Program

mark_execution_end()


# %%


