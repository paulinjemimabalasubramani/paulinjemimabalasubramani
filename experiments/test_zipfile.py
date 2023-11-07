# %%

from zipfile import ZipFile

zip_file = r'C:\myworkdir\temp\ziptest.zip'

zipobj = ZipFile(zip_file, 'r')

zipinfo_list = zipobj.infolist()

# %%



for zipinfo in zipinfo_list:
    print(zipinfo.filename)
    print(zipinfo.is_dir())


