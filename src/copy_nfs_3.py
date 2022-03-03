description = """

Copy NFS files and folders from one location to another

"""


# %% Parse Arguments

if False: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'COPY_NFS',
        'remote_path': r'C:\myworkdir\NFS-CA',
        'source_path_name_and_address': r'C:\myworkdir\Shared\NFS',
        'source_path_position': r'C:\myworkdir\Shared\NFS',
        'source_path_activity': r'C:\myworkdir\Shared\NFS',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, relative_copy_file
from modules3.nfs_header import get_header_info



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_files():
    """
    Copy files and folders to the new location
    """
    for file_name in os.listdir(data_settings.remote_path):
        remote_file_path = os.path.join(data_settings.remote_path, file_name)
        if os.path.isfile(remote_file_path):
            file_name_noext, file_ext = os.path.splitext(file_name)

            if file_ext.lower() != '.dat':
                logger.info(f'Not a .DAT file, not copying: {remote_file_path}')
                continue

            header_info = get_header_info(file_path=remote_file_path)
            if not header_info: continue

            if header_info['key_datetime'] < data_settings.key_datetime:
                logger.info(f'Older date in file name: {remote_file_path} -> skipping copy')
                continue

            source_path_table = 'source_path_' + header_info['table_name_no_firm']
            source_path = os.path.join(getattr(data_settings, source_path_table), header_info['firm_name'])

            relative_copy_file(remote_path=data_settings.remote_path, dest_path=source_path, remote_file_path=remote_file_path)

    print('Finished copying files')



copy_files()



# %% Close Connections / End Program

mark_execution_end()


# %%


