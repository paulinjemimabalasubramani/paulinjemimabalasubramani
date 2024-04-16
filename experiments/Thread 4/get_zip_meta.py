# %%

from zipfile import ZipFile
import os


# %%

#zip_file = r'/opt/EDIP/remote/fasoma05bprd/DownloadData/_TRI/2023/June.zip'
#zip_file = r'C:\myworkdir\temp\temp.zip'

source_path = r'/opt/EDIP/data/nfs_history_load'


# %%


for month in ['Jun', 'Jul', 'Aug']: #  ['Aug', 'Sep', 'Oct']: # ['May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct']:
    zip_file = rf'/opt/EDIP/remote/fasoma05bprd/DownloadData/_SAI/MIPS/2023/{month}.zip'
    print('\n'+zip_file)
    with ZipFile(zip_file, 'r') as zipobj:
        zipinfo_list = zipobj.infolist()
        for zipinfo in zipinfo_list:
            if 'SAI_NFSBOOK_INPUT_' in zipinfo.filename.upper():
                print(f'{zipinfo.file_size/1024:9.3f} {zipinfo.filename}')

                zipobj.extract(member=zipinfo, path=source_path)
                file_path=os.path.join(source_path, zipinfo.filename)



# %%

s = monthly_df.columns[0]
month_str = s[s.find('_'):]

# %%

import pandas as pd

# Sample DataFrame
data = {'col1': [10, 20, 30], 'col2': ['A', 'B', 'C']}
df = pd.DataFrame(data, index=['row1', 'row2', 'row3'])

# Index value to filter by
index_value = 'row2'

# Get the row as a list using either of these methods:

# Method 1: Using loc for label-based indexing
row_as_list = df.loc[index_value].tolist()

# Method 2: Using iloc for integer-based positional indexing
row_as_list = df.iloc[df.index.get_loc(index_value)].tolist()

# Print the filtered row as a list
print(row_as_list)  # Output: [20, 'B']


