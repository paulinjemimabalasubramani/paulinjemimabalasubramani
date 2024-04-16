"""
Read top n lines and save them as txt

"""

# %% Import Libraries

import os




# %% Parameters

folder_path = 'C:\myworkdir\data\envestnet_v35'

allowed_file_extensions = ['.psv']

new_file_name_prefix = 'top_'
new_file_ext = '.txt'
new_folder_path = ''

n_head_lines = 100



# %%

for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    if not os.path.isfile(file_path): continue

    file_name_no_ext, file_ext = os.path.splitext(file_name)
    if not (allowed_file_extensions and file_ext in allowed_file_extensions): continue
    
    new_file_name = new_file_name_prefix + file_name_no_ext + (new_file_ext if new_file_ext else file_ext)
    new_file_path = os.path.join(new_folder_path if new_folder_path else folder_path, new_file_name)

    print(f'Reading from {file_path} -> Writing to {new_file_path}')
    with open(file_path, 'rt') as file:
        with open(new_file_path, 'wt') as new_file:
            for i, line in enumerate(file):
                new_file.write(line)
                if i>=n_head_lines: break



# %%



