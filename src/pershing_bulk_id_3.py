description = """

Add Bulk_id to Fixed Width Files

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_PERSHING_RAA',
        'source_path': r'C:\myworkdir\Shared\PERSHING-ASSETS\RAA',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\pershing_schema',
        }



# %% Import Libraries

import os, sys, tempfile, shutil

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end
from modules3.pershing_header import total_hash_length, hash_func, bulk_file_ext, HEADER_str, TRAILER_str

from distutils.dir_util import remove_tree



# %% Parameters

file_has_header = True
file_has_trailer = True

start_line_record_string = str(data_settings.start_line_record_string)
start_line_pos_start = int(data_settings.start_line_record_position) - 1
start_line_pos_end = start_line_pos_start + len(start_line_record_string)

data_settings.target_path = data_settings.app_data_path
if os.path.isdir(data_settings.target_path): remove_tree(directory=data_settings.target_path, verbose=0, dry_run=0)
os.makedirs(data_settings.target_path, exist_ok=True)



# %% Determine Start Line

@catch_error(logger)
def is_start_line(line:str, file_name:str):
    """
    Determine Start Line
    """
    if file_name.upper().startswith('ISCA'):
        return line[start_line_pos_start-2:start_line_pos_end-2] == start_line_record_string
    else: 
        return line[start_line_pos_start:start_line_pos_end] == start_line_record_string



# %% Add Prefix to Line

@catch_error(logger)
def add_prefix(prefix:str, line:str):
    """
    Add Prefix to Line
    """
    return prefix + ' ' + line



# %% Convert lines to HASH value and write them to file

@catch_error(logger)
def lines_to_hex(ftarget, lines:list):
    """
    Convert lines to HASH value and write them to file
    """
    if len(lines) == 0: return

    hash = hash_func()
    for line in lines:
        hash.update(line.encode('ascii'))
    hex = hash.hexdigest()

    for line in lines:
        ftarget.write(add_prefix(prefix=hex, line=line))



# %% Add Header or Trailer line

@catch_error(logger)
def add_custom_txt_line(ftarget, line:str, txt:str):
    """
    Add Header or Trailer line
    """
    ftarget.write(add_prefix(prefix=txt.ljust(total_hash_length), line=line))



# %% Process single FWF

@catch_error(logger)
def process_single_fwf(source_file_path:str, target_file_path:str, file_name:str):
    """
    Process single FWF
    """
    with open(source_file_path, mode='rt', encoding='utf-8', errors='ignore') as fsource:
        with open(target_file_path, mode='wt', encoding='utf-8') as ftarget:
            first = file_has_header
            lines = []
            for line in fsource:
                if first:
                    add_custom_txt_line(ftarget=ftarget, line=line, txt=HEADER_str)
                    first = False
                else:
                    if is_start_line(line=line, file_name=file_name) and lines:
                        lines_to_hex(ftarget=ftarget, lines=lines)
                        lines = []
                    lines.append(line)

            if file_has_trailer:
                if len(lines)>1: lines_to_hex(ftarget=ftarget, lines=lines[:-1])
                add_custom_txt_line(ftarget=ftarget, line=lines[-1], txt=TRAILER_str)
            else:
                lines_to_hex(ftarget=ftarget, lines=lines)



# %% Main function to iterate over all the files in source_path and add bulk_id

@catch_error(logger)
def iterate_over_all_fwf(source_path:str):
    """
    Main function to iterate over all the files in source_path and add bulk_id
    """
    for root, dirs, files in os.walk(source_path):
        for file_name in files:
            source_file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)
            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                    extract_dir = tmpdir
                    logger.info(f'Extracting {source_file_path} to {extract_dir}')
                    shutil.unpack_archive(filename=source_file_path, extract_dir=extract_dir, format='zip')
                    iterate_over_all_fwf(source_path=extract_dir)
            else:
                target_file_path = os.path.join(data_settings.target_path, file_name + bulk_file_ext)
                logger.info(f'Processing {source_file_path}')
                process_single_fwf(source_file_path=source_file_path, target_file_path=target_file_path, file_name=file_name)

            if hasattr(data_settings, 'delete_files_after') and data_settings.delete_files_after.upper()=='TRUE':
                logger.info(f'Deleting {source_file_path}')
                os.remove(source_file_path)



iterate_over_all_fwf(source_path=data_settings.source_path)



# %% Close Connections / End Program

mark_execution_end()


# %%


