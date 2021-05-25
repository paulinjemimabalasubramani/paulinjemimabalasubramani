""" 
Library Class for retrieving and storing configuration data

"""

# %% libraries

from .common import make_logging

import yaml


# %% Logger


logger = make_logging(__name__)


# %% Main Class
class Config:
    """
    Class for retrieving and storing configuration data
    """
    def __init__(self, file_path:str, defaults:dict={}):
        for name, value in defaults.items():
            setattr(self, name, value) # Write defaults

        try:
            with open(file_path, 'r') as f:
                contents = yaml.load(f, Loader=yaml.FullLoader)
        except Exception as e:
            except_str = f'Error File was not read: {file_path}'
            print(except_str)
            logger.error(except_str, exc_info=True)
            return

        for name, value in contents.items():
            setattr(self, name, value) # Overwrite defaults from file



