"""
Module to handle common file metadata and file operations

"""

# %%

import os, re
from dataclasses import dataclass, field
from collections import OrderedDict
from datetime import datetime
from typing import List, Dict

from .logger import logger, catch_error
from .settings import Config, normalize_name
from .common import get_separator



# %%

@dataclass
class FileMeta:
    """
    Store common File Meta data
    """
    table_name_with_schema:str
    file_path:str = None
    zip_file_path:str = None
    columns:str = None
    delimiter:str = None
    database_name:str = None
    server_name:str = None
    file_type:str = None
    is_full_load:bool = None
    date_of_data:datetime = None
    source_server:str = None
    source_database:str = None
    source_table_name_with_schema:str = None
    pipeline_key:str = None
    additional_info:OrderedDict = field(default_factory=OrderedDict)
    rows_copied:int = None


    def __post_init__(self):
        """
        Runs after __init__
        """
        self.file_name = os.path.basename(self.file_path) if self.file_path else None
        self.zip_file_name = os.path.basename(self.zip_file_path) if self.zip_file_path else None
        self.run_date = logger.run_date.start


    @catch_error()
    def add_config(self, config:Config):
        """
        Add metadata from config and other default values
        """
        if self.database_name is None and hasattr(config, 'target_database'): self.database_name = config.target_database
        if self.server_name is None  and hasattr(config, 'target_server'): self.server_name = config.target_server
        if self.pipeline_key is None: self.pipeline_key = config.pipeline_key
        if self.date_of_data is None: self.date_of_data = logger.run_date.start
        if self.is_full_load is None and hasattr(config, 'is_full_load'): self.is_full_load = config.is_full_load.upper() == 'TRUE'



# %%

@catch_error()
def allowed_file_extensions(config:Config):
    """
    Retrieve Allowed file extensions
    """
    if not hasattr(config, 'allowed_file_extensions'): return

    if isinstance(config.allowed_file_extensions, str):
        ext_regex = r'[^a-zA-Z0-9]'
        ext_list = config.allowed_file_extensions.lower().split(',')
        ext_list = [re.sub(ext_regex, '', e) for e in ext_list]
        config.allowed_file_extensions = ['.'+e for e in ext_list if e]
    elif isinstance(config.allowed_file_extensions, List):
        pass
    else:
        raise ValueError(f'Error in config.allowed_file_extensions:{config.allowed_file_extensions} Invalid Type: {type(config.allowed_file_extensions)}')

    return config.allowed_file_extensions



# %%

def normalize_table_name(table_name_raw:str, config:Config):
    """
    Normalize table_name, add prefix (if any) and schema
    """
    if hasattr(config, 'table_prefix') and config.table_prefix.strip():
        prefix = config.table_prefix.strip().lower() + '_'
        if not table_name_raw.lower().startswith(prefix):
            table_name_raw = prefix + table_name_raw.lower()

    table_name_with_schema = normalize_name(name=config.target_schema) + '.' + normalize_name(name=table_name_raw)
    return table_name_with_schema.lower()




# %%

@catch_error()
def extract_table_name_and_date_from_file_name(file_path:str, config:Config, zip_file_path:str=None):
    """
    Extract table_name and date of data from file_name
    """
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)
    file_name_noext = file_name_noext.lower()

    allowed_file_ext = allowed_file_extensions(config=config)
    if allowed_file_ext and file_ext.lower() not in allowed_file_ext:
        logger.warning(f'Only {allowed_file_ext} extensions are allowed: {file_path}')
        return None, None

    if hasattr(config, 'file_name_regex'):
        match = re.search(config.file_name_regex, file_name_noext)
        if match:
            try:
                table_name_raw = match.group(1)
                if hasattr(config, 'file_date_format'):
                    file_date_str = match.group(2) # Assuming that date will be 2nd
            except Exception as e:
                logger.warning(f'Invalid Match, could not retrieve table_name or file_date from regex pattern: {file_path}. {str(e)}')
                return None, None
        else:
            logger.warning(f'Invalid Match, Could not find date stamp for the file or invalid file name: {file_path}')
            return None, None
    else:
        if hasattr(config, 'file_date_format'):
            date_loc = -file_name_noext[::-1].find('_')
            if date_loc>=0:
                logger.warning(f'Could not find date stamp for the file or invalid file name: {file_path}')
                return None, None
            file_date_str = file_name_noext[date_loc:]
            table_name_raw = file_name_noext[:date_loc-1]
        else:
            table_name_raw = file_name_noext

    if hasattr(config, 'file_date_format'):
        date_of_data = datetime.strptime(file_date_str, config.file_date_format)
    else:
        date_of_data = logger.run_date.start

    table_name_with_schema = normalize_table_name(table_name_raw=table_name_raw, config=config)

    return table_name_with_schema, date_of_data



# %%

@catch_error()
def get_csv_file_columns_and_delimiter(file_path:str, default_data_type:str):
    """
    Get list of columns and delimiter used for csv file
    """
    # Read the header from the CSV file
    with open(file=file_path, mode='rt', encoding='utf-8-sig', errors='ignore') as f:
        HEADER = f.readline()

    delimiter = get_separator(header_string=HEADER)

    columns = HEADER.split(delimiter)
    columns = OrderedDict([(normalize_name(c), default_data_type) for c in columns])

    return columns, delimiter



# %%

@catch_error()
def get_file_meta_csv(file_path:str, config:Config, zip_file_path:str=None):
    """
    Extract file metadata
    """
    file_type = 'csv'

    columns, delimiter = get_csv_file_columns_and_delimiter(file_path=file_path, default_data_type=config.default_data_type)
    if not columns: return

    table_name_with_schema, date_of_data = extract_table_name_and_date_from_file_name(file_path=file_path, config=config, zip_file_path=zip_file_path)
    if not table_name_with_schema: return

    file_meta = FileMeta(
        table_name_with_schema = table_name_with_schema,
        file_path = file_path,
        zip_file_path = zip_file_path,
        columns = columns,
        delimiter = delimiter,
        file_type = file_type,
        date_of_data = date_of_data,
    )

    file_meta.add_config(config=config)

    return file_meta



# %%


