"""
Add Bulk_id to Fixed Width Files

"""


# %% Parse Arguments

if False: # Set to False for Debugging
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
from datetime import datetime

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, is_pc




# %% Parameters




# %%

print(data_settings.source_path)
print(data_settings.target_path)

# %%

sha1 = hashlib.sha1()
sha1.update(b'a')
hex = sha1.hexdigest()

print(hex)
print(len(hex))



# %%

os.makedirs(data_settings.target_path, exist_ok=True)

for root, dirs, files in os.walk(data_settings.source_path):
    for file_name in files:
        source_file_path = os.path.join(root, file_name)
        target_file_path = os.path.join(data_settings.target_path, file_name+'.bulk')





# %%

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



# %%

with open(source_file_path, 'rt') as fsource:
    with open(target_file_path, 'wt') as ftarget:
        first = True
        for line in fsource:
            if first:
                lines_to_hex(ftarget=ftarget, lines=[line])
                first = False
                lines = []
            else:
                if line[2] == 'A':
                    lines_to_hex(ftarget=ftarget, lines=lines)
                    lines = []
                lines.append(line)

        if len(lines)>1: lines_to_hex(ftarget=ftarget, lines=lines[:-1])
        lines_to_hex(ftarget=ftarget, lines=[lines[-1]])





# %%



