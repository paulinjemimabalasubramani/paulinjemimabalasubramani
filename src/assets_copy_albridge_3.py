description = """

Copy Albridge files from remote location to source location

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_COPY_ALBRIDGE',
        'remote_path': r'C:\myworkdir\ALBRIDGE',
        'source_path': r'C:\myworkdir\Shared\ALBRIDGE',
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



# %% Get Financial Institution ID Map

@catch_error(logger)
def get_fid_map():
    """
    Get Financial Institution ID Map
    """
    fid_map = dict()

    fids = data_settings.fid_map.split(',')

    for fid in fids:
        fid_split = fid.split(':')
        firm = fid_split[0].strip().upper()
        idx = int(fid_split[1].strip())
        fid_map = {**fid_map, firm:idx}

    return fid_map



fid_map = get_fid_map()



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

            if file_ext.lower() != '.zip':
                logger.info(f'Not a zip file, not copying: {remote_file_path}')
                continue

            if file_name_noext[0].upper() != 'S':
                logger.info(f'zip file {remote_file_path} should start with "S", skipping copy')
                continue

            file_date_str = file_name_noext[-8:]
            key_datetime = datetime.strptime(file_date_str, data_settings.date_format)
            if key_datetime < data_settings.key_datetime:
                logger.info(f'Older date in file name: {remote_file_path} -> skipping copy')
                continue

            for firm, fin_inst_id in fid_map.items():
                file_fid = file_name_noext[1:len(str(fin_inst_id))+1]
                if file_fid == str(fin_inst_id):
                    source_path = os.path.join(data_settings.source_path, firm)
                    relative_copy_file(remote_path=data_settings.remote_path, dest_path=source_path, remote_file_path=remote_file_path)
                    break

    print('Finished copying files')



copy_files()



# %% Close Connections / End Program

mark_execution_end()


# %%


