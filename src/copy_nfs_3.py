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
        'remote_path': r'C:\myworkdir\NFS',
        'source_path_nabase': r'C:\myworkdir\Shared\NFS-CA',
        'source_path_positd': r'C:\myworkdir\Shared\NFS-ASSETS',
        'source_path_actvyd': r'C:\myworkdir\Shared\NFS-ASSETS',
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



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_files():
    """
    Copy files and folders to the new location
    """
    if not data_settings.remote_path:
        logger.warning('Remote path does not exist')
        return

    if not os.path.isdir(data_settings.remote_path):
        logger.warning(f'Not a folder path: {data_settings.remote_path}')
        return

    for file_name in os.listdir(data_settings.remote_path):
        remote_file_path = os.path.join(data_settings.remote_path, file_name)
        if os.path.isfile(remote_file_path):
            file_name_noext, file_ext = os.path.splitext(file_name)

            if '_' not in file_name_noext: continue
            _index = file_name_noext.index('_')
            headerrecordclientid = file_name_noext[:_index].upper()
            file_name_no_firm_no_ext = file_name_noext[_index+1:].upper()

            if headerrecordclientid not in headerrecordclientid_map:
                logger.warning(f'Unknown headerrecordclientid: {remote_file_path}')
                continue

            if file_ext.lower() != '.dat':
                logger.warning(f'Not a .DAT file, not copying: {remote_file_path}')
                continue

            firm_name = headerrecordclientid_map[headerrecordclientid]

            source_path_table = 'source_path_' + file_name_no_firm_no_ext.lower()
            if not hasattr(data_settings, source_path_table):
                logger.warning(f'{source_path_table} is not defined in SQL PipelineConfig for {remote_file_path}')
                continue

            source_path = os.path.join(getattr(data_settings, source_path_table), firm_name)

            relative_copy_file(remote_path=data_settings.remote_path, dest_path=source_path, remote_file_path=remote_file_path)

    logger.info(f'Finished copying all files')



copy_files()



# %% Close Connections / End Program

mark_execution_end()


# %%


