# %%

from zipfile import ZipFile


# %%


zip_file = r'C:\myworkdir\temp\temp.zip'



# %%


with ZipFile(zip_file, 'r') as zipobj:
    zipinfo_list = zipobj.infolist()
    for zipinfo in zipinfo_list:
        print(f'{zipinfo.file_size/1024:9.3f} {zipinfo.filename}')



# %%



