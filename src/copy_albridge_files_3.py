description = """

Copy Albridge files and folders from one location to another

"""


# %% Import Libraries

import os

from distutils.file_util import copy_file



# %% Parameters

remote_path = ''

source_path_root = ''


fid_map = {
    'FSC': 107,
    'WFS': 163,
    'RAA': 119,
    'SAI': 1134,
    'SPF': 130,
    'TRI': 1293,
}




# %% Copy files and folders to the new location

def copy_files(remote_path:str, source_path:str, fin_inst_id:int):
    """
    Copy files and folders to the new location
    """
    print(f'Copying files {fin_inst_id} from {remote_path} to {source_path}')

    for root, dirs, files in os.walk(remote_path):
        for file_name in files:
            file_name_noext, file_ext = os.path.splitext(file_name)
            remote_file_path = os.path.join(root, file_name)
            source_file_path = os.path.join(source_path, os.path.relpath(root, remote_path), file_name)

            if file_ext.lower() != '.zip':
                print(f'Not a zip file, not copying: {remote_file_path}')
                continue

            if file_name_noext[0].upper() != 'S':
                print(f'zip file {remote_file_path} should start with "S", skipping copy')
                continue

            file_fid = file_name_noext[1:len(str(fin_inst_id))+1]
            if file_fid != str(fin_inst_id):
                continue

            print(f'Copying a file from {remote_file_path} to {source_file_path}')
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

    print('Finished copying files')



# %%

for firm, fin_inst_id in fid_map.items():
    copy_files(
        remote_path = remote_path,
        source_path = os.path.join(source_path_root, firm),
        fin_inst_id = fin_inst_id
    )



# %%


