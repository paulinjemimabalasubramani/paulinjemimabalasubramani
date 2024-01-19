# %%

from zipfile import ZipFile


# %%


zip_file = r''



# %%


with ZipFile(zip_file, 'r') as zipobj:
    zipinfo_list = zipobj.infolist()
    for zipinfo in zipinfo_list:






