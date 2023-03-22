description = """

Copy L&R IFX files from remote location to source location

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--is_zip', help='Check to copy zip files only or all the files. Possible Values: Y/N', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'FP_MIGRATE_LR_IFX',
        'is_zip': 'Y',
        'remote_path': r'C:\myworkdir\data\IFX_LNR',
        'source_path': r'C:\myworkdir\data\IFX_LNR\output',
        'pipeline_firm': 'IFX',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, relative_copy_file

import tempfile, shutil
from datetime import datetime



# %% Get Folder Levels

max_folder_name = r'{{ MAX }}'

folder_levels = [r'%Y%m%d']



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



# %%

@catch_error(logger)
def remove_last_line_from_file(file_path:str):
    """
    Remove last line from CSV / PSV file. Last line has EOF word
    """
    text_seek = 'EOF|'

    with open(file_path, "r+", encoding = "utf-8") as file:
        file.seek(0, os.SEEK_END)
        pos = file.tell() - len(text_seek)
        while pos > 0:
            pos -= 1
            file.seek(pos, os.SEEK_SET)
            text = file.read(len(text_seek))
            if text==text_seek:
                break

        if pos > 0:
            file.seek(pos, os.SEEK_SET)
            file.truncate()



# %%

@catch_error(logger)
def unzip_and_custom_copy(remote_file_path:str, source_path:str):
    """
    Unzip the file from remote location to a temporary location. Remove the last EOF line from CSV/PSV file. And copy it to final location
    """
    logger.info(f'Unzip and Custom Copy with EOF removed: From: {remote_file_path} To: {source_path}')

    with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as extract_dir:
        logger.info(f'Extracting {remote_file_path} to {extract_dir}')
        shutil.unpack_archive(filename=remote_file_path, extract_dir=extract_dir, format='zip')

        for extract_file_name in os.listdir(extract_dir):
            extract_file_path = os.path.join(extract_dir, extract_file_name)
            if os.path.isfile(extract_file_path):
                file_name_noext, file_ext = os.path.splitext(extract_file_path)
                if file_ext.lower() not in ['.txt', '.csv']:
                    logger.warning(f'Unknown file extention, skipping: {extract_file_path}')
                    continue
                remove_last_line_from_file(file_path=extract_file_path)
                relative_copy_file(remote_path=extract_dir, dest_path=source_path, remote_file_path=extract_file_path)



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

            if data_settings.is_zip.upper() in ['Y', 'YES', 'TRUE', '1'] and file_ext.lower() != '.zip':
                logger.warning(f'Not a .zip file, not copying: {remote_file_path}')
                continue

            unzip_and_custom_copy(remote_file_path=remote_file_path, source_path=data_settings.source_path)

    logger.info(f'Finished copying files for firm: {firm}')



# %% Copy Files

@catch_error(logger)
def copy_files_per_firm():
    """
    Copy Files
    """
    latest_remote_path = find_latest_folder(remote_path=data_settings.remote_path)
    if latest_remote_path:
        logger.info(f'Latest remote path: {latest_remote_path}')
        copy_files(firm=data_settings.pipeline_firm.upper(), firm_remote_path=latest_remote_path)
    else:
        logger.warning(f'No latest remote path found for {data_settings.remote_path}')



copy_files_per_firm()



# %% Close Connections / End Program

mark_execution_end()


# %%


