description = """

Copy Pershing files from remote location to source location

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'COPY_PERSHING',
        'remote_path_raa': r'C:\myworkdir\PERSHING',
        'remote_path_spf': r'C:\myworkdir\PERSHING-SPF',
        'source_path_acct': r'C:\myworkdir\Shared\PERSHING-CA',
        'source_path_accf': r'C:\myworkdir\Shared\PERSHING-CA',
        'source_path_gact': r'C:\myworkdir\Shared\PERSHING-ASSETS',
        'source_path_gcus': r'C:\myworkdir\Shared\PERSHING-ASSETS',
        'source_path_gmon': r'C:\myworkdir\Shared\PERSHING-ASSETS',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, relative_copy_file, collect_keys_from_config



# %% Collect Source Paths

source_paths = collect_keys_from_config(prefix='source_path_', uppercase_key=True)



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

            if file_name_noext.upper() not in source_paths:
                logger.info(f'File {file_name} is not in registered names: {list(source_paths)}')
                continue

            if file_ext.lower() != '.zip':
                logger.warning(f'Not a .zip file, not copying: {remote_file_path}')
                continue

            source_path = os.path.join(source_paths[file_name_noext.upper()], firm)

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
        copy_files(firm=firm, firm_remote_path=firm_remote_path)



copy_files_per_firm()



# %% Close Connections / End Program

mark_execution_end()


# %%


