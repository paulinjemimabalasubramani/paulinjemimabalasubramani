"""
Add Bulk_id to Fixed Width Files

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description='Migrate any CSV type files with date info in file name')

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'CA_CONVERT_PERSHING_CUSTOMERACCOUNT_RAA',
        'source_path': r'C:\myworkdir\Shared\PERSHING\23131',
        'target_path': r'C:\myworkdir\Shared\PERSHING\23131_bulk'
        }



# %% Import Libraries

import os, sys
import hashlib

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end




# %% Parameters

bulk_file_ext = '.bulk'
file_has_header = True
file_has_trailer = True

is_start_line = lambda line: line[2] == 'A' # Determine Start Line



# %% Convert lines to SHA1 value and write them to file

@catch_error(logger)
def lines_to_hex(ftarget, lines:list):
    """
    Convert lines to SHA1 value and write them to file
    """
    if len(lines) == 0: return

    sha1 = hashlib.sha1()
    for line in lines:
        sha1.update(line.encode('ascii'))
    hex = sha1.hexdigest()

    for line in lines:
        ftarget.write(hex + ' ' + line)



# %% Process single FWF

@catch_error(logger)
def process_single_fwf(source_file_path:str, target_file_path:str):
    """
    Process single FWF
    """
    with open(source_file_path, 'rt') as fsource:
        with open(target_file_path, 'wt') as ftarget:
            first = file_has_header
            lines = []
            for line in fsource:
                if first:
                    lines_to_hex(ftarget=ftarget, lines=[line])
                    first = False
                else:
                    if is_start_line(line=line):
                        lines_to_hex(ftarget=ftarget, lines=lines)
                        lines = []
                    lines.append(line)

            if file_has_trailer:
                if len(lines)>1: lines_to_hex(ftarget=ftarget, lines=lines[:-1])
                lines = [lines[-1]]

            lines_to_hex(ftarget=ftarget, lines=lines)



# %% Main function to iterate over all the files in source_path and add bulk_id

@catch_error(logger)
def iterate_over_all_fwf():
    """
    Main function to iterate over all the files in source_path and add bulk_id
    """
    logger.info({
        'source_path': data_settings.source_path,
        'target_path': data_settings.target_path,
        })

    os.makedirs(data_settings.target_path, exist_ok=True)

    for root, dirs, files in os.walk(data_settings.source_path):
        for file_name in files:
            source_file_path = os.path.join(root, file_name)
            target_file_path = os.path.join(data_settings.target_path, file_name + bulk_file_ext)
            logger.info(f'Processing {source_file_path}')
            process_single_fwf(source_file_path=source_file_path, target_file_path=target_file_path)



iterate_over_all_fwf()



# %% Close Connections / End Program

mark_execution_end()


# %%


