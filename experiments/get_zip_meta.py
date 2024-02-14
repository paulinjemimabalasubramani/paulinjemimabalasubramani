# %%

from zipfile import ZipFile
import os


# %%

#zip_file = r'/opt/EDIP/remote/fasoma05bprd/DownloadData/_TRI/2023/June.zip'
#zip_file = r'C:\myworkdir\temp\temp.zip'

source_path = r'/opt/EDIP/data/nfs_history_load'


# %%


for month in ['Aug', 'Sep', 'Oct']: # ['May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct']:
    zip_file = rf'/opt/EDIP/remote/fasoma05bprd/DownloadData/_TRI/2023/{month}.zip'
    print('\n'+zip_file)
    with ZipFile(zip_file, 'r') as zipobj:
        zipinfo_list = zipobj.infolist()
        for zipinfo in zipinfo_list:
            if '_NAME' in zipinfo.filename.upper():
                print(f'{zipinfo.file_size/1024:9.3f} {zipinfo.filename}')

                zipobj.extract(member=zipinfo, path=source_path)
                file_path=os.path.join(source_path, zipinfo.filename)



# %%



