description = """

Generic Code to delete old log files and folders recursively

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'MAINTENANCE_AIRFLOW_LOG_CLEANUP',
        'default_log_days': '30',
        'log_path_1': r'C:\myworkdir\Logs',
        'log_days_1': '25',
        }



# %% Import Libraries

import os, sys, time

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, collect_keys_from_config



# %% Parameters



# %% Delete log files recursively

@catch_error(logger)
def delete_log_files(log_path:str, log_days:int):
    """
    Delete log files recursively
    """
    logger.info(f'Deleting files older than {log_days} days at {log_path}')
    for root, dirs, files in os.walk(log_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            mtime = os.path.getmtime(file_path)
            last_modified_days = (time.time() - mtime) / 60 / 60 / 24

            if last_modified_days > log_days:
                logger.info(f'Deleting file {file_path}. Last modified {last_modified_days} days ago.')
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.warning(f'Error in deleting file {file_path}. {str(e)}')



# %% Delete empty folders

@catch_error(logger)
def delete_empty_folders(folder_path:str):
    """
    Delete empty folders
    """
    logger.info(f'Deleting empty folders at {folder_path}')
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            try:
                if len(os.listdir(dir_path)) == 0:
                    logger.info(f'Removing Empty Folder {dir_path}')
                    os.removedirs(dir_path)
            except Exception as e:
                logger.warning(f'Error in deleting folder {dir_path}. {str(e)}')



# %% Delete old log files and folders recursively

@catch_error(logger)
def log_cleanup():
    """
    Delete old log files and folders recursively
    """
    log_paths = collect_keys_from_config(prefix='log_path_', uppercase_key=True)
    log_days = collect_keys_from_config(prefix='log_days_', uppercase_key=True)

    for log_key, log_path in log_paths.items():
        if not os.path.isdir(log_path):
            logger.warning(f'Not a folder path: {log_path}')
            continue

        log_day = int(log_days[log_key]) if log_key in log_days else int(data_settings.default_log_days)

        delete_log_files(log_path=log_path, log_days=log_day)
        delete_empty_folders(folder_path=log_path)


log_cleanup()



# %% Close Connections / End Program

mark_execution_end()


# %%


