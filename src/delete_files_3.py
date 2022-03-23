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
        'delete_files_after': 'TRUE',
        'source_path': r'C:\myworkdir\Shared\ALBRIDGE\WFS',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end
from modules3.migrate_files import file_meta_exists_for_select_files



# %% Parameters



# %% Delete files and folders in the source path

@catch_error(logger)
def delete_files():
    """
    Delete files and folders in the source path
    """
    if not hasattr(data_settings, 'delete_files_after') or data_settings.delete_files_after.upper()!='TRUE':
        logger.info('DELETE_FILES_AFTER config is not enabled - Skipping Delete')
        return

    if not os.path.isdir(data_settings.source_path):
        logger.info(f'Directory {data_settings.source_path} is not found - Skipping Delete')
        return

    logger.info(f'Deleting files from {data_settings.source_path}')

    for root, dirs, files in os.walk(data_settings.source_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_meta_exists_for_select_files(file_path=file_path):
                logger.info(f'Deleting file: {file_path}')
                os.remove(file_path)

    logger.info('Finished deleting files')



delete_files()



# %% Close Connections / End Program

mark_execution_end()


# %%


