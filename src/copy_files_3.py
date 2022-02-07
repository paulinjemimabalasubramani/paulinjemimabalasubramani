description = """

Generic Code to copy files and folders from one location to another

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
        'remote_path': r'C:\myworkdir\ALBRIDGE\WFS',
        'source_path': r'C:\myworkdir\Shared\ALBRIDGE\WFS',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end

from distutils.dir_util import copy_tree



# %% Parameters



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_files():
    """
    Copy files and folders to the new location
    """
    logger.info(f'Copying files from {data_settings.remote_path} to {data_settings.source_path}')
    copy_tree( # https://docs.python.org/2/distutils/apiref.html#distutils.dir_util.copy_tree
        src = data_settings.remote_path, # Files in src that begin with .nfs are skipped
        dst = data_settings.source_path,
        preserve_mode = 0, # Do not copy file’s mode (type and permission bits)
        preserve_times = 0, # Do not copy file's last-modified and last-access times
        preserve_symlinks = 0, # the destination of the symlink will be copied
        update = 0, # Update files regardless if they already exists
        verbose = 0, # Do not ignore errors
        dry_run = 0,
    )
    logger.info('Finished copying files')

copy_files()



# %% Close Connections / End Program

mark_execution_end()


# %%


