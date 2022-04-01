description = """

Copy NFS SAI files from remote location to source location

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'COPY_NFS_SAI',

        'clientid': '133',

        'remote_path_positd': r'C:\myworkdir\NFS',
        'remote_path_actvyd': r'C:\myworkdir\NFS',
        'remote_path_secmaster': r'C:\myworkdir\NFS',
        'remote_path_trdrev': r'C:\myworkdir\NFS',
        'remote_path_acctbald': r'C:\myworkdir\NFS',

        'source_path_positd': r'C:\myworkdir\Shared\NFS-ASSETS',
        'source_path_actvyd': r'C:\myworkdir\Shared\NFS-ASSETS',
        'source_path_secmaster': r'C:\myworkdir\Shared\NFS-ASSETS',
        'source_path_trdrev': r'C:\myworkdir\Shared\NFS-ASSETS',
        'source_path_acctbald': r'C:\myworkdir\Shared\NFS-ASSETS',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, relative_copy_file, collect_keys_from_config

from datetime import datetime



# %% Parameters

firm_name = data_settings.pipeline_firm.upper()
clientid = data_settings.clientid



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_file(file_type:str, remote_path:str):
    """
    Copy files and folders to the new location
    """
    if not os.path.isdir(remote_path):
        logger.warning(f'Remote path does not exist: {remote_path}')
        return

    source_path_setting = 'source_path_' + file_type.lower()
    if not hasattr(data_settings, source_path_setting):
        logger.warning(f'{source_path_setting} is not defined in SQL PipelineConfig for file_type {file_type}')
        return

    files = {}
    for file_name in os.listdir(remote_path):
        remote_file_path = os.path.join(remote_path, file_name)
        if not os.path.isfile(remote_file_path): continue

        file_name_noext, file_ext = os.path.splitext(file_name)

        if file_ext.lower() != '.txt':
            logger.warning(f'Not a .txt file, not copying: {remote_file_path}')
            continue

        try:
            file_date = datetime.strptime(file_name_noext[-8:], r"%m_%d_%y")
        except:
            logger.warning(f'Unable parse date from file name: {file_name}')
            continue
        files[file_date] = remote_file_path

    if not files:
        logger.warning(f'No files found in {remote_path}')
        return

    remote_file_path = files[max(files)]
    source_file_name = (clientid + '_' + file_type + '.DAT').upper()
    source_file_path = os.path.join(getattr(data_settings, source_path_setting), firm_name, source_file_name)

    relative_copy_file(remote_path=remote_path, dest_path=source_file_path, remote_file_path=remote_file_path)

    logger.info(f'Finished copying {remote_file_path} to {source_file_path}')



# %% Copy Files per Type

@catch_error(logger)
def copy_files_per_type():
    """
    Copy Files per type
    """
    remote_paths = collect_keys_from_config(prefix='remote_path_', uppercase_key=True)

    for file_type, remote_path in remote_paths.items():
        copy_file(file_type=file_type, remote_path=remote_path)



copy_files_per_type()



# %% Close Connections / End Program

mark_execution_end()


# %%


