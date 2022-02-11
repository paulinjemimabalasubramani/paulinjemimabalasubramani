description = """

Add Bulk_id to Fixed Width Files

"""


# %% Parse Arguments

if False: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'CA_MIGRATE_NFS',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\nfs_schema',
        'source_path': r'C:\myworkdir\Shared\NFS-CA',
        }



# %% Import Libraries

from multiprocessing.sharedctypes import Value
import os, sys, tempfile, shutil, json

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end

from distutils.dir_util import remove_tree



# %% Parameters

file_has_header = True
file_has_trailer = True

HEADER_record = 'H'
DATA_record = 'D'
TRAILER_record = 'T'

data_settings.target_path = data_settings.app_data_path
if os.path.isdir(data_settings.target_path): remove_tree(directory=data_settings.target_path, verbose=0, dry_run=0)
os.makedirs(data_settings.target_path, exist_ok=True)

json_file_ext = '.json'



# %% Determine Start Line

@catch_error(logger)
def is_start_line(line:str):
    """
    Determine Start Line
    """
    record_segment = line[0:1]
    record_number = line[14:17]
    return record_segment == '1' and record_number == '101'



# %%







# %% Process all lines belonging to a single record

@catch_error(logger)
def process_lines(ftarget, lines:list):
    """
    Process all lines belonging to a single record
    """
    if len(lines) == 0: return

    fba = ()
    for line in lines:
        record_segment = line[0:1]
        if record_segment not in ['1', '2', '3', '4', '5']: raise ValueError(f'Invalid record_segment: {record_segment}')

        if record_segment == '1':
            firm = line[1:5]
            branch = line[5:8]
            account_number = line[8:14]
            record_number = line[14:17]
            if record_number == '101':
                fba = (firm, branch, account_number)
            else:
                if fba != (firm, branch, account_number):
                    raise ValueError(f'Values {(firm, branch, account_number)} in record {record_number} does not Match record 101 data {fba}')

        if record_number == '900': continue # record_number 900 is empty - ignore







# %% Add Header or Trailer line

@catch_error(logger)
def process_custom_line(ftarget, line:str, custom:str):
    """
    Add Header or Trailer line
    """
    #ftarget.write(add_prefix(prefix=txt.ljust(total_hash_length), line=line))



# %% Process single FWF

@catch_error(logger)
def process_single_fwf(source_file_path:str, target_file_path:str):
    """
    Process single FWF
    """
    with open(source_file_path, mode='rt', encoding='ISO-8859-1') as fsource:
        with open(target_file_path, mode='wt', encoding='utf-8') as ftarget:
            first = file_has_header
            lines = []
            for line in fsource:
                if first:
                    process_custom_line(ftarget=ftarget, line=line, custom=HEADER_record)
                    first = False
                else:
                    if is_start_line(line=line) and lines:
                        process_lines(ftarget=ftarget, lines=lines)
                        lines = []
                    lines.append(line)

            if file_has_trailer:
                if len(lines)>1: process_lines(ftarget=ftarget, lines=lines[:-1])
                process_custom_line(ftarget=ftarget, line=lines[-1], custom=TRAILER_record)
            else:
                process_lines(ftarget=ftarget, lines=lines)



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
                continue

            target_file_path = os.path.join(data_settings.target_path, file_name + json_file_ext)
            logger.info(f'Processing {source_file_path}')
            process_single_fwf(source_file_path=source_file_path, target_file_path=target_file_path)



iterate_over_all_fwf(source_path=data_settings.source_path)



# %% Close Connections / End Program

mark_execution_end()


# %%


