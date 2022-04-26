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

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, relative_copy_file

from datetime import datetime



# %% Parameters

firm_name = data_settings.pipeline_firm.upper()
clientid = data_settings.clientid
remote_path_suffix = data_settings.path_suffix



# %% Get Folder Levels

suffix_folder_list = remote_path_suffix.split('/0231/')
max_folder_name = r'{{ MAX }}'

folder_levels = [r'%Y', r'%b', r'%Y%m%d', suffix_folder_list[0], max_folder_name ,suffix_folder_list[1]]



# %% Find Latest Folder

@catch_error(logger)
def find_latest_folder(remote_path:str, level:int=0):
    """
    Find Latest Folder
    """
    if level >= len(folder_levels): return remote_path

    paths = {}
    for file_name in os.listdir(remote_path):
        remote_file_path = os.path.join(remote_path, file_name)
        if os.path.isdir(remote_file_path):
            if folder_levels[level][0] == '%':
                try:
                    path_key = datetime.strptime(file_name, folder_levels[level])
                except:
                    continue
            elif folder_levels[level] == max_folder_name:
                path_key = file_name
            else:
                path_key = folder_levels[level]
                remote_file_path = os.path.join(remote_path, folder_levels[level])
                paths[path_key] = remote_file_path
                break

            paths[path_key] = remote_file_path

    if not paths:
        raise ValueError(f'No Paths found in {remote_path}')

    return find_latest_folder(remote_path=paths[max(paths)], level=level+1)



remote_path = find_latest_folder(remote_path=data_settings.remote_path)
logger.info(f'Latest path: {remote_path}')



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_file(remote_path:str):
    """
    Copy files and folders to the new location
    """
    if not os.path.isdir(remote_path):
        raise ValueError(f'Remote path does not exist: {remote_path}')

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


