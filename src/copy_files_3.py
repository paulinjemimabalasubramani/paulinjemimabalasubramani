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
from distutils.file_util import copy_file



# %% Parameters



# %% Copy files and folders to the new location

@catch_error(logger)
def copy_files():
    """
    Copy files and folders to the new location
    """
    if not hasattr(data_settings, 'remote_path') or not data_settings.remote_path:
        logger.info('Remote Path Empty, Skipping Copy Files')
        return

    if hasattr(data_settings, 'copy_only_files') and data_settings.copy_only_files:
        logger.info(f'Copying files {data_settings.copy_only_files} from {data_settings.remote_path} to {data_settings.source_path}')

        copy_only_files = data_settings.copy_only_files.lower().split(',')
        copy_only_files = [c.strip() for c in copy_only_files]
        for root, dirs, files in os.walk(data_settings.remote_path):
            for file_name in files:
                remote_file_path = os.path.join(root, file_name)
                source_file_path = os.path.join(data_settings.source_path, os.path.relpath(root, data_settings.remote_path), file_name)

                if any(c in file_name for c in copy_only_files):
                    logger.info(f'Copying a file from {remote_file_path} to {source_file_path}')
                    copy_file( # https://docs.python.org/3/distutils/apiref.html#distutils.file_util.copy_file
                        src = remote_file_path,
                        dst = source_file_path,
                        preserve_mode = 0, # Do not copy file’s mode (type and permission bits)
                        preserve_times = 1, # Copy file's last-modified and last-access times
                        update = 1, #  src will only be copied if dst does not exist, or if dst does exist but is older than src.
                        link = None, # if it is None (the default), files are copied
                        verbose = 0, # Do not ignore errors
                        dry_run = 0,
                    )
                else:
                    logger.info(f'{file_name} is not in [{data_settings.copy_only_files}] -> Skipping Copy')

    else:
        logger.info(f'Copying files from {data_settings.remote_path} to {data_settings.source_path}')
        copy_tree( # https://docs.python.org/3/distutils/apiref.html#distutils.dir_util.copy_tree
            src = data_settings.remote_path, # Files in src that begin with .nfs are skipped
            dst = data_settings.source_path,
            preserve_mode = 0, # Do not copy file’s mode (type and permission bits)
            preserve_times = 1, # Copy file's last-modified and last-access times
            preserve_symlinks = 0, # the destination of the symlink will be copied
            update = 1, #  src will only be copied if dst does not exist, or if dst does exist but is older than src.
            verbose = 0, # Do not ignore errors
            dry_run = 0,
        )
    logger.info('Finished copying files')



copy_files()



# %% Close Connections / End Program

mark_execution_end()


# %%


