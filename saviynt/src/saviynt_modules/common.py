"""
Library for common generic functions

"""

# %% Import Libraries

import os, subprocess, re, json
from collections import OrderedDict
from datetime import datetime
from typing import List, Dict

from .logger import logger, catch_error



# %% Parameters

common_delimiter = '|'
name_regex:str = r'[\W]+'



# %%

def clean_delimiter_value_for_bcp(value:str):
    """
    remove common delimiter values and carriage returns, so that BCP tool can read the string correctly
    """
    return re.sub(r'\s', ' ', re.sub(r'\|', ':', value), flags=re.MULTILINE)


# %%

def normalize_name(name:str):
    """
    Clean up name and make it standard looking
    """
    name = re.sub(r'\[|\]|\"', '', str(name).lower().strip())
    name = re.sub(name_regex, '_', str(name).strip())

    if name and name[0].isdigit():
        name = '_' + name

    return name



# %%

def clean_columns(columns:List) -> List:
    """
    Clean up column names, sort out duplicates, empty columns and bad column names (e.g columns start with number, or SQL reserved names etc)
    """
    bad_column_name = 'column'
    reserved_names = ['select', 'delete', 'null', 'create', 'update', 'insert']
    columns = [normalize_name(c) for c in columns]

    columnsx = []
    bad_column_count = 0
    for col in columns:
        if not col:
            while True:
                bad_column_count += 1
                bad_column = normalize_name(bad_column_name + str(bad_column_count))
                if bad_column not in columnsx and bad_column not in columns:
                    break
            columnsx.append(bad_column)
            logger.warning(f'Empty column name found, renamed to "{bad_column}"')
        elif col in columnsx or col in reserved_names:
            duplicate_column_count = 0
            while True:
                duplicate_column_count += 1
                duplicate_column = normalize_name(col + str(duplicate_column_count))
                if duplicate_column not in columnsx and duplicate_column not in columns:
                    break
            columnsx.append(duplicate_column)
            logger.warning(f'Duplicate or reserved column name "{col}" found, renamed to "{duplicate_column}"')
        else:
            columnsx.append(col)

    return columnsx



# %%

@catch_error()
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

catch_error()
def run_process(command:str, mask_error:bool=False, hint:str=''):
    """
    Run command line process. Returns None if error.
    """
    encoding = 'UTF-8'
    command_regex = r'\r?\n' # remove line endings
    command = re.sub(command_regex, ' ', command) # remove line endings

    process = subprocess.Popen(
        args = command,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        shell = True,
        )

    stdout, stderr = '', ''

    k = 0
    while k<2:
        out = process.stdout.read().decode(encoding).rstrip('\n')
        err = process.stderr.read().decode(encoding).rstrip('\n')

        if out: print(out)
        if err: print(err)
        stdout += out
        stderr += err

        if process.poll() is not None:
            k += 1

    #stdout, stderr = process.communicate()
    #stdout, stderr = stdout.decode(encoding), stderr.decode(encoding)

    if process.returncode != 0:
        if mask_error:
            logger.error(f'Error in running run_process, returncode={process.returncode}, hint={hint}')
        else:
            logger.error(f'Error in running command: {command}')
            logger.error(stderr)
        return None

    return stdout



# %%

@catch_error()
def remove_square_parenthesis(table_name_with_schema:str):
    """
    Remove square parenthesis from table_name_with_schema (for comparison purposes)
    """
    return re.sub(r'\[|\]', '', table_name_with_schema)



# %%

@catch_error()
def get_separator(header_string:str):
    """
    Find out what separator is used in the file header
    """
    separators = ['!#!#', '|', '\t', ',']
    delimiter = common_delimiter
    for s in separators:
        if s in header_string:
            delimiter = s
            break
    return delimiter



# %%

@catch_error()
def to_sql_value(cval):
    """
    Utility function to convert Python values to SQL server equivalent values
    """
    strftime = r'%Y-%m-%d %H:%M:%S'  # http://strftime.org/

    if cval is None:
        cval = 'NULL'
    elif isinstance(cval, datetime):
        cval = f"'{cval.strftime(strftime)}'"
    elif isinstance(cval, bool):
        cval = str(int(cval))
    elif isinstance(cval, int) or isinstance(cval, float):
        cval = str(cval)
    elif isinstance(cval, dict) or isinstance(cval, OrderedDict) or isinstance(cval, list):
        if cval:
            cval = f"'{json.dumps(cval)}'"
        else:
            cval = 'NULL'
    else:
        cval = str(cval).replace("'", "''")
        cval = f"'{cval}'"

    return cval


# %%

@catch_error()
def picture_to_decimals(pic:str) -> int:
    """
    Returns number of decimals in a field from schema picture (used in NFS and Pershing files)
    """
    pic_list = pic.replace(' ','').upper().split('V')

    if len(pic_list)<=1:
        decimals = 0
    else:
        d = pic_list[-1]
        if any(x.isalpha() for x in d):
            decimals = 0
        elif '(' in d:
            dx = d.split('(')
            dx = dx[1].split(')')
            decimals = int(dx[0])
        else:
            decimals = d.count('9')

    return decimals



# %%


