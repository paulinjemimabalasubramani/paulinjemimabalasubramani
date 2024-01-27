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
from .common import get_separator, to_sql_value



# %%

@dataclass
class FileMeta:
    """
    Store common File Meta data
    """
    table_name_with_schema:str
    file_path:str = None
    zip_file_path:str = None
    columns:OrderedDict = field(default_factory=OrderedDict)
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
    file_size_kb:float = None
    file_modified_date:datetime = None
    zip_file_size_kb:float = None
    zip_file_modified_date:datetime = None
    load_time_seconds:float = None


    @catch_error()
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
        if self.server_name is None and hasattr(config, 'target_server'): self.server_name = config.target_server
        if self.pipeline_key is None: self.pipeline_key = config.pipeline_key
        if self.date_of_data is None: self.date_of_data = logger.run_date.start
        if self.is_full_load is None and hasattr(config, 'is_full_load'): self.is_full_load = config.is_full_load.upper() == 'TRUE'


    @catch_error()
    def get_elt_load_history_columns(self):
        """
        get list of columns for in elt.load_history
        """
        class sql_types:
            id = 'INT IDENTITY'
            str = 'NVARCHAR(2000)'
            int = 'INT'
            datetime = 'DATETIME'
            decimal2 = 'NUMERIC(38,2)'
            json = 'NVARCHAR(2000)'

        elt_columns = OrderedDict([
            ('id', sql_types.id),
            ('table_name_with_schema', sql_types.str),
            ('database_name', sql_types.str),
            ('server_name', sql_types.str),
            ('is_full_load', sql_types.int),
            ('rows_copied', sql_types.int),
            ('load_time_seconds', sql_types.decimal2),
            ('date_of_data', sql_types.datetime),
            ('run_date', sql_types.datetime),
            ('pipeline_key', sql_types.str),
            ('file_type', sql_types.str),
            ('delimiter', sql_types.str),
            ('columns', sql_types.str),
            ('file_name', sql_types.str),
            ('file_path', sql_types.str),
            ('file_size_kb', sql_types.decimal2),
            ('file_modified_date', sql_types.datetime),
            ('zip_file_name', sql_types.str),
            ('zip_file_path', sql_types.str),
            ('zip_file_size_kb', sql_types.decimal2),
            ('zip_file_modified_date', sql_types.datetime),
            ('source_server', sql_types.str),
            ('source_database', sql_types.str),
            ('source_table_name_with_schema', sql_types.str),
            ('additional_info', sql_types.json),
        ])

        return elt_columns


    @catch_error()
    def add_file_os_info(self):
        """
        """
        self.file_size_kb = os.path.getsize(filename=self.file_path) / 1024.0
        self.file_modified_date = datetime.fromtimestamp(os.path.getmtime(filename=self.file_path))

        if self.zip_file_path:
            self.zip_file_size_kb = os.path.getsize(filename=self.zip_file_path) / 1024.0
            self.zip_file_modified_date = datetime.fromtimestamp(os.path.getmtime(filename=self.zip_file_path))


    @catch_error()
    def get_elt_values_sql(self):
        """
        Get SQL version of ELT Values
        """
        elt_columns = self.get_elt_load_history_columns()

        elt_values = OrderedDict()
        for elt_column in elt_columns:
            if elt_column == 'id':
                continue # id field will be auto-populated (autoincrement)

            val = getattr(self, elt_column)

            if elt_column == 'columns':
                val = ','.join([x for x in val])
                val = f"'{val[:2000]}'" # Trucate columns values longer than 2000 characters
            else:
                val = to_sql_value(val)

            elt_values[elt_column] = val

        return elt_values



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
            logger.warning(f'Invalid Match, Could not find date stamp for the file or invalid file name: {file_path}\nfile_name_regex={config.file_name_regex}  file_name_noext={file_name_noext}')
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
    elif zip_file_path:
        date_of_data = datetime.fromtimestamp(os.path.getmtime(filename=zip_file_path))
    else:
        date_of_data = datetime.fromtimestamp(os.path.getmtime(filename=file_path))

    table_name_with_schema = normalize_table_name(table_name_raw=table_name_raw, config=config)

    date_of_data = date_of_data.replace(microsecond=0)
    return table_name_with_schema, date_of_data



# %%

def clean_columns(columns:List) -> List:
    """
    Clean up column names, sort out duplicates, empty columns and bad column names (e.g columns start with number, or SQL reserved names etc)
    """
    bad_column_name = 'column'
    columns = [normalize_name(c) for c in columns]

    columnsx = []
    bad_column_count = 0
    duplicate_column_count = 0
    for col in columns:
        if col and col not in columnsx:
            columnsx.append(col)
        elif not col:
            while True:
                bad_column_count += 1
                bad_column = normalize_name(bad_column_name + str(bad_column_count))
                if bad_column not in columnsx and bad_column not in columns:
                    break
            columnsx.append(bad_column)
            logger.warning(f'Empty column name found, renamed to "{bad_column}"')
        elif col in columnsx:
            while True:
                duplicate_column_count += 1
                duplicate_column = normalize_name(col + str(duplicate_column_count))
                if duplicate_column not in columnsx and duplicate_column not in columns:
                    break
            columnsx.append(duplicate_column)
            logger.warning(f'Duplicate column name "{col}" found, renamed to "{duplicate_column}"')

    return columnsx



# %%

@catch_error()
def get_file_columns_csv(file_path:str, default_data_type:str):
    """
    Get list of columns and delimiter used for csv file
    """
    # Read the header from the CSV file
    with open(file=file_path, mode='rt', encoding='utf-8-sig', errors='ignore') as f:
        HEADER = f.readline()

    delimiter = get_separator(header_string=HEADER)
    columns = HEADER.split(delimiter)
    columns = clean_columns(columns=columns)
    columns = OrderedDict([(c, default_data_type) for c in columns])

    return columns, delimiter



# %%

@catch_error()
def get_file_meta_csv(file_type:str, file_path:str, config:Config, zip_file_path:str=None):
    """
    Extract file metadata for csv-like files
    """
    columns, delimiter = get_file_columns_csv(file_path=file_path, default_data_type=config.default_data_type)
    if not columns: return

    if hasattr(config, 'columns') and sorted(list(config.columns.keys()))==sorted(list(columns.keys())):
        columns = config.columns # to apply right column type

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
    file_meta.add_file_os_info()

    return file_meta



# %%

@catch_error()
def get_file_meta_json(file_type:str, file_path:str, config:Config, zip_file_path:str=None):
    """
    Extract file metadata for json-like files
    """
    columns = config.columns # Assume columns will be available from previous process.
    if not columns:
        logger.warning(f'No columns found for file: {file_path}')
        return

    table_name_with_schema, date_of_data = extract_table_name_and_date_from_file_name(file_path=file_path, config=config, zip_file_path=zip_file_path)
    if not table_name_with_schema: return

    file_meta = FileMeta(
        table_name_with_schema = table_name_with_schema,
        file_path = file_path,
        zip_file_path = zip_file_path,
        columns = columns,
        file_type = file_type,
        date_of_data = date_of_data,
    )

    file_meta.add_config(config=config)
    file_meta.add_file_os_info()

    return file_meta



# %%

file_type_map_fn = {
    'csv': get_file_meta_csv,
    'json': get_file_meta_json,
}



# %%

@catch_error()
def get_file_meta(file_type:str, file_path:str, config:Config, zip_file_path:str=None) -> FileMeta:
    """
    Main function to get file_meta
    """
    get_file_meta_fn = file_type_map_fn[file_type]
    file_meta = get_file_meta_fn(file_type=file_type, file_path=file_path, config=config, zip_file_path=zip_file_path)
    return file_meta



# %%


