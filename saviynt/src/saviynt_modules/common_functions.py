"""
Library for common generic functions

"""


# %% Import Libraries

import os
from .logging_module import logger, catch_error



# %%

@catch_error(logger)
def remove_last_line_from_file(file_path:str, last_line_text_seek:str):
    """
    Remove last line from text file.
    """
    with open(file_path, 'r+', encoding = 'utf-8-sig') as file:
        file.seek(0, os.SEEK_END)
        pos = file.tell() - len(last_line_text_seek)
        while pos > 0:
            pos -= 1
            file.seek(pos, os.SEEK_SET)
            text = file.read(len(last_line_text_seek))
            if text==last_line_text_seek:
                break

        if pos > 0:
            file.seek(pos, os.SEEK_SET)
            file.truncate()



# %%


