description = """

Copy NFS files from remote location to source location

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'COPY_NFS',
        'remote_path_raa': r'C:\myworkdir\NFS',
        'remote_path_spf': r'C:\myworkdir\NFS',
        'source_path_name_and_address': r'C:\myworkdir\Shared\NFS-CA',
        'source_path_position': r'C:\myworkdir\Shared\NFS-ASSETS',
        'source_path_activity': r'C:\myworkdir\Shared\NFS-ASSETS',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, relative_copy_file, collect_keys_from_config
from modules3.nfs_header import headerrecordclientid_map


# %% Parameters

file_name_map = {
    'NABASE': 'name_and_address',
    'POSITD': 'position',
    'ACTVYD': 'activity',
}



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_files(firm:str, headerrecordclientid:str, firm_remote_path:str):
    """
    Copy files and folders to the new location
    """
    if not os.path.isdir(firm_remote_path):
        logger.warning(f'Not a folder path: {firm_remote_path}')
        return

    logger.info({
        'firm': firm,
        'headerrecordclientid': headerrecordclientid,
        'firm_remote_path': firm_remote_path,
        })

    allowed_files = dict()
    for k, v in file_name_map.items():
        if k.upper()=='NABASE':
            allowed_files = {**allowed_files, '_'.join([headerrecordclientid, k]):v}
        else:
            allowed_files = {**allowed_files, k:v}

    for file_name in os.listdir(firm_remote_path):
        remote_file_path = os.path.join(firm_remote_path, file_name)
        if os.path.isfile(remote_file_path):
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_name_noext not in allowed_files: continue

            if file_ext.lower() != '.dat':
                logger.warning(f'Not a .DAT file, not copying: {remote_file_path}')
                continue

            source_path_table = 'source_path_' + allowed_files[file_name_noext]
            if not hasattr(data_settings, source_path_table):
                logger.warning(f'{source_path_table} is not defined in SQL PipelineConfig')
                continue

            source_path = os.path.join(getattr(data_settings, source_path_table), firm)

            relative_copy_file(remote_path=firm_remote_path, dest_path=source_path, remote_file_path=remote_file_path)

    logger.info(f'Finished copying files for firm: {firm}')



# %% Copy Files per firm

@catch_error(logger)
def copy_files_per_firm():
    """
    Copy Files per firm
    """
    remote_paths = collect_keys_from_config(prefix='remote_path_', uppercase_key=True)

    for firm, firm_remote_path in remote_paths.items():
        headerrecordclientid = [k for k, v in headerrecordclientid_map.items() if v.upper()==firm.upper()][0].upper()
        copy_files(firm=firm, headerrecordclientid=headerrecordclientid, firm_remote_path=firm_remote_path)



copy_files_per_firm()



# %% Close Connections / End Program

mark_execution_end()


# %%


