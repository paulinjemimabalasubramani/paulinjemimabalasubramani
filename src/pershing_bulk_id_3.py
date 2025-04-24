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
        'source_path': r'C:\myworkdir\data\pershing3',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\pershing_schema',
        }



# %% Import Libraries

import os, sys, tempfile, shutil

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end
from modules3.pershing_header import total_hash_length, hash_func, bulk_file_ext, HEADER_str, TRAILER_str, get_header_info

from distutils.dir_util import remove_tree
from raa_expanded_accf_file_split import split_accf_files



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
def is_start_line(line:str, header_line:str):
    """
    Determine Start Line
    """
    if header_line.upper().find('EXPANDED SEC DESC')>=0:
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
        hash.update(line.encode('utf-8'))
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
def process_single_fwf(source_file_path:str, target_file_path:str):
    """
    Process single FWF
    """
    header_info = get_header_info(file_path=source_file_path, is_bulk_formatted=False)

    with open(source_file_path, mode='rt', encoding='utf-8', errors='replace') as fsource:
        if header_info['form_name'] == 'SECURITY PROFILES':
            split_files = dict()
            for line in fsource:
                if line[:3] == 'BOF':
                    header_line = line
                elif line[:3] == 'EOF':
                    tralier_line = line
                elif line[:2] == 'SP':
                    record_name = line[2]
                else:
                    logger.warning(f'Not a valid SPAT file for line: {line}')

                if line[:2] == 'SP':
                    if record_name not in split_files:
                        header_line1 = header_line.replace('SECURITY PROFILES ', 'SECURITY PROFILE '+record_name)
                        file_name = os.path.basename(source_file_path) + '_' + record_name
                        target_file_path = os.path.join(data_settings.target_path, file_name + bulk_file_ext)
                        split_files[record_name] = open(target_file_path, mode='wt', encoding='utf-8')
                        add_custom_txt_line(ftarget=split_files[record_name], line=header_line1, txt=HEADER_str)

                    lines_to_hex(ftarget=split_files[record_name], lines=[line])

            for record_name in split_files:
                add_custom_txt_line(ftarget=split_files[record_name], line=tralier_line, txt=TRAILER_str)
                split_files[record_name].close()

        else: # not SECURITY PROFILES
            with open(target_file_path, mode='wt', encoding='utf-8') as ftarget:
                first = file_has_header
                header_line = ''
                lines = []
                for line in fsource:
                    if first:
                        add_custom_txt_line(ftarget=ftarget, line=line, txt=HEADER_str)
                        header_line = line
                        first = False
                    else:
                        if (is_start_line(line=line, header_line=header_line) or 'asset_transfer' in file_name ) and lines:
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
            #As we are splitting the ACA2.ACA2 into transfer and asset file we are excluding ACA2.ACA2 for further processing. Please refer pershing_process_multiline_files.py
            if(file_name == 'ACA2.ACA2'): continue
            source_file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)
            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                    extract_dir = tmpdir
                    logger.info(f'Extracting {source_file_path} to {extract_dir}')
                    shutil.unpack_archive(filename=source_file_path, extract_dir=extract_dir, format='zip')
                    splitAccountFullFile(source_path,source_file_path,zip_extract_dir=extract_dir)
                    iterate_over_all_fwf(source_path=extract_dir)
            else:
                target_file_path = os.path.join(data_settings.target_path, file_name + bulk_file_ext)
                logger.info(f'Processing {source_file_path} , target file path {target_file_path}')
                process_single_fwf(source_file_path=source_file_path, target_file_path=target_file_path)

            if hasattr(data_settings, 'delete_files_after') and data_settings.delete_files_after.upper()=='TRUE':
                logger.info(f'Deleting {source_file_path}')
                os.remove(source_file_path)

def splitAccountFullFile(source_path:str,source_file_path:str,zip_extract_dir:str):
    # data_settings.split_only_files defined in Pipelineconfiguration table
    if data_settings.get_value('split_only_files',None) and os.path.exists(zip_extract_dir+'/'+data_settings.split_only_files):
        logger.info(f'splitAccountFullFile : source_path -> {source_path} , source_file_path -> {source_file_path} , zip_extract_dir -> {zip_extract_dir}')
        # Split the zip_extract_dir/ACCF.ACCF into multiple chunk
        split_accf_files(zip_extract_dir+'/'+data_settings.split_only_files)
        # Remove zip_extract_dir/ACCF.ACCF
        os.remove(zip_extract_dir+'/'+data_settings.split_only_files)

iterate_over_all_fwf(source_path=data_settings.source_path)



# %% Close Connections / End Program

mark_execution_end()


# %%


