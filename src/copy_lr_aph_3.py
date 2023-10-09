description = """

Copy L&R APH files from remote location to source location

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'FP_MIGRATE_LR_APH',
        'remote_path': r'C:\myworkdir\data\APH_LNR',
        'source_path': r'C:\myworkdir\data\APH_LNR\output',
        'pipeline_firm': 'APH',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, relative_copy_file, find_latest_folder



# %% Get Folder Levels

folder_levels = [r'%Y%m%d']



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_files(firm:str, firm_remote_path:str):
    """
    Copy files and folders to the new location
    """
    if not os.path.isdir(firm_remote_path):
        logger.warning(f'Not a folder path: {firm_remote_path}')
        return

    for file_name in os.listdir(firm_remote_path):
        remote_file_path = os.path.join(firm_remote_path, file_name)
        if os.path.isfile(remote_file_path):
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() not in ['.txt', '.csv', '.psv']:
                logger.warning(f'Unknown file extention, skipping: {remote_file_path}')
                continue

            relative_copy_file(remote_path=os.path.dirname(remote_file_path), dest_path=data_settings.source_path, remote_file_path=remote_file_path, dest_is_folder=True)


    logger.info(f'Finished copying files for firm: {firm}')



# %% Copy Files

@catch_error(logger)
def copy_files_per_firm():
    """
    Copy Files
    """
    latest_remote_path = find_latest_folder(remote_path=data_settings.remote_path, folder_levels=folder_levels)
    if latest_remote_path:
        logger.info(f'Latest remote path: {latest_remote_path}')
        copy_files(firm=data_settings.pipeline_firm.upper(), firm_remote_path=latest_remote_path)
    else:
        logger.warning(f'No latest remote path found for {data_settings.remote_path}')



copy_files_per_firm()



# %% Close Connections / End Program

mark_execution_end()


# %%


