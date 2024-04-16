description = """

Copy Envestnet files

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_ENVESTNET',
        'remote_path': r'C:\myworkdir\data\envestnet', # /opt/EDIP/remote/APP01/ftproot/LocalUser/wmp/envestnet_edip
        'source_path': r'C:\myworkdir\Shared\envestnet',
        'schema_file_copy_path': r'C:\myworkdir\EDIP-Code\config\envestnet\envestnet_files.csv',
        }



# %% Import Libraries

import os, sys, re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, get_csv_rows

from datetime import datetime
from zipfile import ZipFile



# %% Parameters



# %%

@catch_error(logger)
def get_copy_schema():
    """
    Get copy schema - i.e. list of file names and versions to copy
    """
    copy_schema = dict()
    for row in get_csv_rows(csv_file_path=data_settings.schema_file_copy_path):
        copy_schema[row['name'].lower()] = row['version'].lower()

    return copy_schema



copy_schema = get_copy_schema()



# %%

@catch_error(logger)
def get_zip_files():
    """
    Get the latest zip files to extract data, based on latest date
    """
    zip_files = []
    zip_dates = []

    for file_name in os.listdir(data_settings.remote_path):
        remote_file_path = os.path.join(data_settings.remote_path, file_name)
        if os.path.isfile(remote_file_path):
            file_name_noext, file_ext = os.path.splitext(file_name)

            date_str = file_name_noext[-8:]
            if file_ext.lower()!='.zip' or not date_str.isdigit(): continue

            try:
                _ = datetime.strptime(date_str, r'%Y%m%d')
            except:
                logger.warning(f'Not an invalid zip file date: {remote_file_path}')
                continue

            zip_files.append(remote_file_path)
            zip_dates.append(file_name_noext[-8:])

    maxdate = max(zip_dates)
    zip_files = [zip_files[i] for i, x in enumerate(zip_dates) if x == maxdate]
    return zip_files



zip_files = get_zip_files()



# %%

@catch_error(logger)
def parse_envestnet_file_name(file_name, zip_name):
    """
    Parse Envestnet file name and return clean names
    """
    file_name_noext, file_ext = os.path.splitext(os.path.basename(file_name))
    date_str = file_name_noext[-8:]
    if file_ext.lower()!='.psv' or not date_str.isdigit(): 
        logger.warning(f'Not an valid file: {file_name} in zip archive {zip_name} -> skipping file')
        return None, None

    try:
        _ = datetime.strptime(date_str, r'%Y%m%d')
    except:
        logger.warning(f'Not an invalid file date: {file_name} in zip archive {zip_name} -> skipping file')
        return None, None

    file_name_noext = re.sub('_+', '_', file_name_noext[:-8])[:-1].lower()
    if file_name_noext[:3].lower() != 'ag_':
        logger.warning(f'File does not start with "AG_": {file_name} in zip archive {zip_name} -> skipping file')
        return None, None

    file_name_split = file_name_noext.split('_')

    if not len(file_name_split) in [2, 3]:
        logger.warning(f'Cannot parse file name: {file_name} in zip archive {zip_name} -> skipping file')
        return None, None

    if len(file_name_split) == 2:
        file_name = file_name_split[1]
        file_version = 'x'
    else:
        file_name = file_name_split[2]
        file_version = file_name_split[1]

    return file_name, file_version



# %%

@catch_error(logger)
def extract_zip_files():
    """
    Extract selected zip files according to copy schema / file version.
    """
    for zip_file in zip_files:
        with ZipFile(zip_file, 'r') as zipobj:
            zipinfo_list = zipobj.infolist()
            for zipinfo in zipinfo_list:
                if zipinfo.is_dir(): continue

                file_name, file_version = parse_envestnet_file_name(file_name=zipinfo.filename, zip_name=zip_file)
                if not file_name: continue

                if not copy_schema.get(file_name):
                    logger.warning(f'File name doesn''t exist in Envestnet schema, unknown file name: {zipinfo.filename} in zip archive {zip_file} -> skipping file')
                    continue

                if copy_schema[file_name] != file_version: continue

                logger.info(f'Extracting {zipinfo.filename} from {zip_file} to {data_settings.source_path}')
                zipobj.extract(member=zipinfo, path=data_settings.source_path)



extract_zip_files()



# %% Close Connections / End Program

mark_execution_end()


# %%


