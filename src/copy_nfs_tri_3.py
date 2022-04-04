description = """

Copy NFS TRI files from remote location to source location

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'COPY_NFS_TRI',

        'clientid': 'TR1',

        'remote_path': r'C:\myworkdir\NFS',
        'path_suffix': f'suffix',

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
remote_path_suffix = data_settings.path_suffix

date_levels = [r'%Y', r'%b', r'%Y%m%d']




# %% Find Latest Folder

@catch_error(logger)
def find_latest_folder(remote_path:str, level:int=0):
    """
    Find Latest Folder
    """
    if level >= len(date_levels): return remote_path

    paths = {}
    for file_name in os.listdir(remote_path):
        remote_file_path = os.path.join(remote_path, file_name)
        if os.path.isdir(remote_file_path):
            try:
                dt = datetime.strptime(file_name, date_levels[level])
            except:
                continue
            paths[dt] = remote_file_path

    if not paths:
        logger.warning(f'No Paths found in {remote_path}')
        return

    return find_latest_folder(remote_path=paths[max(paths)], level=level+1)



remote_path = find_latest_folder(remote_path=data_settings.remote_path)
logger.info(f'Latest path: {remote_path}')



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_file(remote_path:str):
    """
    Copy files and folders to the new location
    """
    if not remote_path: return
    remote_path = os.path.join(remote_path, remote_path_suffix)

    if not os.path.isdir(remote_path):
        logger.warning(f'Remote path does not exist: {remote_path}')
        return

    for file_name in os.listdir(remote_path):
        remote_file_path = os.path.join(remote_path, file_name)
        if not os.path.isfile(remote_file_path): continue

        file_name_noext, file_ext = os.path.splitext(file_name)

        if file_ext.lower() != '.txt':
            logger.warning(f'Not a .txt file, not copying: {remote_file_path}')
            continue

        source_path_setting = 'source_path_' + file_name_noext.lower()
        if not hasattr(data_settings, source_path_setting):
            logger.warning(f'{source_path_setting} is not defined in SQL PipelineConfig for {remote_file_path}')
            continue

        source_file_name = (clientid + '_' + file_name_noext + '.DAT').upper()
        source_file_path = os.path.join(getattr(data_settings, source_path_setting), firm_name, source_file_name)

        relative_copy_file(remote_path=remote_path, dest_path=source_file_path, remote_file_path=remote_file_path)

    logger.info(f'Finished copying {remote_file_path} to {source_file_path}')



copy_file(remote_path=remote_path)



# %% Close Connections / End Program

mark_execution_end()


# %%


