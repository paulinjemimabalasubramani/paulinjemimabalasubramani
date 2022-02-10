description = """

Generic Code to delete files and folders recursively

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_ALBRIDGE_WFS',
        'delete_files_after': '1',
        'source_path': r'C:\myworkdir\Shared\ALBRIDGE\WFS',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end

from distutils.dir_util import remove_tree



# %% Parameters



# %% Delete files and folders to the new location

@catch_error(logger)
def delete_files():
    """
    Delete files and folders to the new location
    """
    if not hasattr(data_settings, 'delete_files_after') or data_settings.delete_files_after.upper()!='TRUE':
        logger.info('DELETE_FILES_AFTER config is not enabled - Skipping Delete')
    elif not os.path.isdir(data_settings.source_path):
        logger.info(f'Directory {data_settings.source_path} is not found - Skipping Delete')
    else:
        logger.info(f'Deleting files from {data_settings.source_path}')
        remove_tree( # https://docs.python.org/2/distutils/apiref.html#distutils.dir_util.remove_tree
            directory = data_settings.source_path,
            verbose = 0, # Do not ignore errors
            dry_run = 0,
        )
        logger.info('Finished deleting files')


delete_files()



# %% Close Connections / End Program

mark_execution_end()


# %%


