"""
Centralized settings management, read settings from various sources
Metadata driven coding

"""

# %% Import Libraries

import yaml, csv, re, platform, psutil, os, logging
from typing import List, Dict
from collections import OrderedDict
from datetime import datetime
from typing import List, Dict

from .logger import logger, catch_error, get_env, environment
from .connections import Connection



# %% Parameters / Constants

config_folder_path:str = './saviynt/config'
name_regex:str = r'[\W]+'



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
    def read_environment(self, env_var_names:List=[], raise_error_if_no_value:bool=False):
        """
        Read Environmental Variables
        """
        for variable_name in env_var_names:
            variable_value = get_env(variable_name=variable_name, raise_error_if_no_value=raise_error_if_no_value)
            if variable_value is not None:
                setattr(self, variable_name.lower().strip(), variable_value)


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
    generic_key = 'generic'


    @catch_error()
    def __init__(self, env_var_names:List=[], config_file_path:str=None, **kwargs):
        """
        Initiate class - build on top of base class initialization
        """
        super().__init__(defaults={})

        self.system_info = system_info()
        self.read_environment(env_var_names=env_var_names)
        self.set_values(values=kwargs)

        self.filter_list = [self.generic_key]
        if hasattr(self, 'pipeline_key'):
            self.filter_list.append(self.pipeline_key)

        if config_file_path:
            self.config_file_path = config_file_path
            self.read_csv(file_path=self.config_file_path, filter_list=self.filter_list)


    @catch_error()
    def add_connection_from_config(self, prefix:str):
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

            dict_map |= {key:val}

        return dict_map



# %%

@catch_error()
def get_config(**kwargs):
    """
    Create Config object with args
    Add ELT and Target connections to config object
    """
    env_var_names = []

    config_file_path = os.path.join(config_folder_path, f'config_{environment.ENVIRONMENT.lower()}.csv')

    config = Config(env_var_names=env_var_names, config_file_path=config_file_path, **kwargs)

    for connection_prefix in ['elt', 'target']:
        config.add_connection_from_config(prefix=connection_prefix)

    if hasattr(config, 'date_threshold'):
        config.date_threshold = datetime.strptime(config.date_threshold, r'%Y-%m-%d')

    return config



# %%

@catch_error()
def init_app(__file__:str, __description__:str='Data Migration', args:Dict={}, test_pipeline_key:str=Config.generic_key):
    """
    First procedure to run
    """
    if environment.environment >= environment.qa:
        import argparse
        parser = argparse.ArgumentParser(description=__description__)
        parser.add_argument('--pipeline_key', '--pk', help='pipeline_key value for getting pipeline settings', required=True)
        args2 = parser.parse_args().__dict__
    else:
        args2 = {
            'pipeline_key': test_pipeline_key,
            }

    args2 |= args
    config = get_config(**args2)

    logger.set_logger(
        logging_level = getattr(logging, config.logging_level, logging.INFO),
        app_name = os.path.basename(__file__),
        log_folder_path = getattr(config, 'log_folder_path', logger.log_folder_path),
        )

    logger.info(config.__dict__) # print out all settings

    for c in ['pipeline_key', 'msteams_webhook_url']:
        if hasattr(config, c):
            logger.msteams_webhook_url = getattr(config, c)

    return config



# %%


