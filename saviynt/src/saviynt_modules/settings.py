"""
Centralized settings management, read settings from various sources
Metadata driven coding

"""

# %% Import Libraries

import yaml, csv, re, platform, psutil, os
from typing import List, Dict
from collections import OrderedDict

from .logger import logger, catch_error, get_env, environment



# %% Parameters / Constants

name_regex = r'[\W]+'



# %%

@catch_error()
def system_info(logger=None):
    """
    Get System Info as JSON
    """
    uname = platform.uname()

    sysinfo = {
        'Python_Version': platform.python_version(),
        'Operating_System': uname.system,
        'Network_Name': uname.node,
        'OS_Release': uname.release,
        'OS_Version': uname.version,
        'Machine_Type': uname.machine,
        'Processor': uname.processor,
        'RAM': str(round(psutil.virtual_memory().total / (1024.0 **3))) + ' GB',
        'pwd': os.path.realpath('.'),
    }
    return sysinfo



# %%

@catch_error()
def normalize_name(name:str):
    """
    Clean up name and make it standard looking
    """
    return re.sub(name_regex, '_', str(name).lower().strip())



# %% Get CSV rows

@catch_error()
def get_csv_rows(csv_file_path:str, csv_encoding:str='UTF-8-SIG', normalize_names:bool=True, delimiter:str=','):
    """
    Generator function to get csv file rows one by one
    """
    with open(file=csv_file_path, mode='rt', newline='', encoding=csv_encoding, errors='ignore') as csvfile:
        reader = csv.DictReader(f=csvfile, delimiter=delimiter)
        for row in reader:
            rowl = OrderedDict()
            for k, v in row.items():
                key = k
                if normalize_names:
                    key = normalize_name(name=key)

                if v is not None:
                    val = str(v).strip()
                else:
                    val = None

                rowl[key] = val
            yield rowl



# %% Base class for Config Class to load data from Config Files

class ConfigBase:
    """
    Class for retrieving and storing configuration data
    """

    @catch_error()
    def __init__(self, defaults:Dict={}):
        """
        Initiate the class.
        Assign defaults if any config data doesn't exist.
        """
        self.set_values(values=defaults)


    @catch_error()
    def set_values(self, values:dict):
        """
        Set the values to class / Overwrite existing
        """
        for name, value in values.items():
            setattr(self, name.lower().strip(), value) # Write defaults


    @catch_error()
    def read_environment(self, env_var_names:List=[]):
        """
        Read Environmental Variables
        """
        for envv in env_var_names:
            setattr(self, envv.lower().strip(), get_env(variable_name=envv))


    @catch_error()
    def read_yaml(self, file_path:str):
        """
        Read settings from YAML file
        """
        with open(file=file_path, mode='rt', encoding='UTF-8', errors='replace') as f:
            contents = yaml.load(f, Loader=yaml.SafeLoader)

        self.set_values(values=contents)


    @catch_error()
    def read_csv(self, file_path:str, filter_list:List=[]):
        """
        Read settings from CSV file
        """
        logger.info(f'Reading settings file {os.path.realpath(file_path)}, Present Working Directory: {os.path.realpath(".")}')
        flist = [x.lower() for x in filter_list]

        contents = {}
        for row in get_csv_rows(csv_file_path=file_path):
            if row['pipeline_key'] in flist or not flist:
                contents |= {row['config_key']:row['config_value']}

        self.set_values(values=contents)


    @catch_error()
    def get_value(self, attr_name:str, default_value:None):
        """
        Get Config value. If value doesn't exist then save default_value and retrieve it.
        """
        if not hasattr(self, attr_name) and default_value is not None:
            setattr(self, attr_name, default_value) 
        return getattr(self, attr_name)


    @catch_error()
    def get_secret(secret_name:str):
        """
        Function to get secret information. Contents will vary depending on where secret is located
        """
        secret = get_env(variable_name=secret_name)
        return secret



# %%

class Config(ConfigBase):
    """
    Building on top of base config
    """
    config_file_path = './saviynt/config/config.csv'
    config_dev_file_path = './saviynt/config/config_dev.csv'
    generic_key = 'generic'

    @catch_error()
    def __init__(self, args:Dict={}, env_var_names:List=[]):
        """
        Initiate class - build on top of base class initialization
        """
        super().__init__(defaults={})

        self.system_info = system_info()
        self.read_environment(env_var_names=env_var_names)
        self.set_values(values=args)

        self.filter_list = [self.generic_key]
        if hasattr(self, 'pipeline_key'):
            self.filter_list.append(self.pipeline_key)

        config_file_path = self.config_file_path if environment.is_prod else self.config_dev_file_path
        self.read_csv(file_path=config_file_path, filter_list=self.filter_list)

        logger.info(self.__dict__) # print out all settings


    @catch_error()
    def add_connection_from_config(self, prefix:str, Connection):
        """
        Add target connection to settings
        """
        prefix = prefix.lower().strip()
        setattr(self, f'{prefix}_connection', Connection.from_config(config=self, prefix=prefix))


    @staticmethod
    @catch_error()
    def convert_string_map_to_dict(map_str:str, uppercase_key:bool=True, uppercase_val:bool=True):
        """
        Convert string mapping to dictionary
        """
        dict_map = dict()
        kvs = map_str.split(',')

        for kv in kvs:
            kv_split = kv.split(':')
            key = kv_split[0].strip()
            val = kv_split[1].strip()

            if not key or not val: continue

            if uppercase_key: key = key.upper()
            if uppercase_val: val = val.upper()

            dict_map = {**dict_map, key:val}

        return dict_map



# %%


